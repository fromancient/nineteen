import asyncio
import json
import os
import httpx
import websocket
import time

import numpy as np
from fiber.chain import chain_utils
from fiber.chain import interface

from fiber.logging_utils import get_logger
from fiber.chain.chain_utils import query_substrate
from fiber.chain.weights import _normalize_and_quantize_weights

from core.utils import download_s3_file
from core.constants import PROD_NETUID, BASE_NINETEEN_API_URL
from core.models.config_models import AuditConfig
from validator.control_node.src.set_weights.calculate_and_schedule_weights import set_weights
from validator.control_node.src.cycle.calculations import calculate_scores_for_settings_weights
from validator.models import PeriodScore, RewardData, Contender
from core.models import config_models as cmodels
from validator.utils.post.nineteen import DataTypeToPost, ValidatorInfoPostBody, post_to_nineteen_ai
from core import constants as ccst
from core.task_config import get_public_task_configs


logger = get_logger(__name__)


_config = None

def load_config() -> AuditConfig:
    global _config
    if _config is None:
        subtensor_network = os.getenv("SUBTENSOR_NETWORK") or 'finney'
        subtensor_address = os.getenv("SUBTENSOR_ADDRESS") or 'wss://entrypoint-finney.opentensor.ai:443'
        wallet_name = os.getenv("WALLET_NAME") or None
        hotkey_name = os.getenv("HOTKEY_NAME") or None
        netuid = os.getenv("NETUID")
        if netuid is None:
            netuid = 176 if subtensor_network == "test" else 19
            logger.warning(f"NETUID not set, using {netuid}")
        else:
            netuid = int(netuid)

        refresh_nodes: bool = os.getenv("REFRESH_NODES", "true").lower() == "true"
        if refresh_nodes:
            try:
                substrate = interface.get_substrate(subtensor_network=subtensor_network, subtensor_address=subtensor_address)
            except websocket._exceptions.WebSocketBadStatusException as e:
                logger.error(f"Failed to get substrate: {e}. Sleeping for 20 seconds and then trying again...")
                time.sleep(20)
                substrate = interface.get_substrate(subtensor_network=subtensor_network, subtensor_address=subtensor_address)
        else:
            # this is only used for testing
            substrate = None
        
        keypair = None 
        if wallet_name and hotkey_name:
            keypair = chain_utils.load_hotkey_keypair(wallet_name=wallet_name, hotkey_name=hotkey_name)
        logger.info(f"This is my own keypair {keypair}")

        httpx_limits = httpx.Limits(max_connections=500, max_keepalive_connections=100)
        timeout = httpx.Timeout(timeout=600.0, connect=60.0)  # 10 minutes
        httpx_client = httpx.AsyncClient(limits=httpx_limits, timeout=timeout)


        _config = AuditConfig(
            substrate=substrate,
            keypair=keypair,
            netuid=netuid,
            httpx_client=httpx_client
        )
    return _config


def _normalised_vector_dot_product(a, b):
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    
    if norm_a == 0 or norm_b == 0:
        logger.error(f"norm_a : {norm_a} - norm_b : {norm_b}")
        raise ValueError("Cannot compute normalized dot product with zero vector")
        
    return sum(a[i] * b[i] for i in range(len(a))) / (norm_a * norm_b)


async def _get_task_results_for_rayon_validator(config: AuditConfig) -> dict[str, dict[str, dict[str, list[RewardData] | list[PeriodScore]]] | list[Contender] | dict[str, cmodels.FullTaskConfig]]:
    """Get task results for a rayon validator."""
    try:
        response = await config.httpx_client.get(BASE_NINETEEN_API_URL + 'v1/auditing/scores-url')

        if response.status_code != 200:
            logger.error(f"Failed to get latest scores url: {response.status_code} {response.text} :(")
            raise Exception(f"Failed to get latest scores url: {response.status_code} {response.text} :(")

        url = response.json()["url"]

        logger.info(f"Getting task results from {url}")
        result_filepath = await download_s3_file(url)
        
        try:
            with open(result_filepath, "r") as f:
                task_results_dicts = json.load(f)
        finally:
            # Clean up the file after reading
            try:
                os.remove(result_filepath)
                logger.info(f"Successfully cleaned up file: {result_filepath}")
            except Exception as e:
                logger.warning(f"Failed to clean up file {result_filepath}: {e}")

        return task_results_dicts
    except Exception as e:
        logger.error(f"Error in _get_task_results_for_rayon_validator: {e}")
        raise


async def get_scores_from_rayon_vali(config: AuditConfig) -> dict:
    """Check that scores are calculated correctly by the validator.

    Receive all the period score & reward data entries (in the past 5 days), contenders and the task config used by the Rayon validator to set weights.
    Check that the scores for these tasks correctly add up to the weights set by the validator
    for all miners on chain.

    This helps to audit validators by:
        - Ensuring the weights set are backed up by scores for tasks
        - Preventing the validator from being able to manipulate scores to set weights on chain, without a task being completed
        - Ensuring miners are rewarded for fair work

    This is one tool, used in conjunction with others.
    Miners (well, everyone) can see every task that runs through the subnet,
    and see their scores using the dashboards on nineteen.ai. Details about the models trained by
    miners are available on there too, so anyone can check the evaluation was fair.

    To make this more robust: we can add a function which sends tasks through to the api, to ensure all organic
    jobs are indeed included in the scoring. For the short term, miners can of course always check this, by comparing the work
    they have done to the tasks on the dashboards - it's only a minor improvement.

    """

    task_results = await _get_task_results_for_rayon_validator(config)

    if not task_results:
        logger.info("There were no results to be scored")
        return {}

    task_results['task_configs'] = {
        key: cmodels.FullTaskConfig.model_validate(value) for key, value in task_results['task_configs'].items()
    }  
    task_results['contenders'] = [Contender.model_validate(x) for x in task_results["contenders"]]

    for task, config in task_results['task_configs'].items():
        task_results['metrics'][task][0]['reward_datas'] = {
            key: [RewardData.model_validate(item) for item in value] for key, value in task_results['metrics'][task][0]['reward_datas'].items()
        }
        task_results['metrics'][task][0]['period_scores'] = {
            key: [PeriodScore.model_validate(item) for item in value] for key, value in task_results['metrics'][task][0]['period_scores'].items()
        }


    return task_results


async def audit_weights(config: AuditConfig) -> bool:
    """Get the weights for a rayon validator."""
    results = await get_scores_from_rayon_vali(config)

    node_ids, node_weights = await calculate_scores_for_settings_weights(config, results)
    node_ids_formatted, node_weights_formatted = _normalize_and_quantize_weights(node_ids, node_weights)

    rayon_weights = [0 for i in range(256)]
    for node_id, weight in zip(node_ids_formatted, node_weights_formatted):
        rayon_weights[node_id] = weight

    # 142 is the Rayon validator's UID on SN19
    substrate, weights = query_substrate(config.substrate, "SubtensorModule", "Weights", [PROD_NETUID, 142])
    weights_values = [0 for i in range(256)]
    for node_id, weight_value in weights:
        weights_values[node_id] = weight_value

    similarity_between_scores = _normalised_vector_dot_product(rayon_weights, weights_values)

    if similarity_between_scores > 0.98:
        logger.info(f"✅ Yay! The scores are similar to the weights set on chain!! Similarity: {similarity_between_scores}")

        if config.keypair:
            logger.info(f"Hotkey and coldkey pair inputted, attempting to find a vali uid on netuid {PROD_NETUID}...")
            _, my_vali_uid = query_substrate(
                substrate, "SubtensorModule", "Uids", [config.netuid, config.keypair.ss58_address], return_value=True
            )

            if my_vali_uid is not None:
                logger.info(f"Found my vali uid on netuid {PROD_NETUID}, setting weights!")
                success = await set_weights(config, node_ids_formatted, node_weights_formatted, my_vali_uid)
                return success
        return True

    else:
        logger.error(
            f"Dear Auditor, the similarity between the scores and the weights set on chain is {similarity_between_scores}."
            "This is quite low, and you might want to look into this!"
        )
        return False


async def post_vali_info(config: AuditConfig):
    versions = str(ccst.VERSION_KEY)+'-auditor' + ':' + '-'
    public_configs = get_public_task_configs()
    if config.keypair:
        try:
            await post_to_nineteen_ai(
                data_to_post=ValidatorInfoPostBody(
                    validator_hotkey=config.keypair.ss58_address,
                    task_configs=public_configs,
                    versions=versions,
                ).model_dump(mode="json"),
                keypair=config.keypair,
                data_type_to_post=DataTypeToPost.VALIDATOR_INFO,
            )
        except Exception as e:
            logger.error(f"Couldn't post data to taovision.ai!")
            logger.exception(e)
    else:
        logger.info(f"Third-party auditing, not posting info to taovision.ai!")

async def main():
    config = load_config()
    substrate = config.substrate

    try:
        uid = None
        if config.keypair:
            substrate, uid = query_substrate(
                substrate,
                "SubtensorModule",
                "Uids",
                [config.netuid, config.keypair.ss58_address],
                return_value=True,
            )
            if uid is None:
                logger.info(f"Can't find hotkey {config.keypair.ss58_address} for our keypair on netuid: {config.netuid}, assuming third-party auditing!")
        else:
            logger.info("Hotkey and coldkey pair not inputted, assuming third-party auditing!")

        while True:
            try:
                await post_vali_info(config)
                substrate, current_block = query_substrate(substrate, "System", "Number", [], return_value=True)
                substrate, last_updated_value = query_substrate(
                    substrate, "SubtensorModule", "LastUpdate", [config.netuid], return_value=False
                )
                if last_updated_value is not None and uid is not None:
                    updated: int = current_block - last_updated_value[uid].value
                    substrate, weights_set_rate_limit = query_substrate(
                        substrate, "SubtensorModule", "WeightsSetRateLimit", [config.netuid], return_value=True
                    )
                    logger.info(
                        f"My Validator Node ID: {uid}. Last updated {updated} blocks ago. Weights set rate limit: {weights_set_rate_limit}."
                    )

                    if updated < weights_set_rate_limit:
                        sleep_duration = (weights_set_rate_limit - updated) * 12
                        logger.info(f"Sleeping for {sleep_duration} seconds [{sleep_duration / 12 } blocks]" " as we set weights recently...")
                        await asyncio.sleep(sleep_duration)
                        continue

                success = await audit_weights(config)
                if success:
                    break

            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(60)
                
    finally:
        await config.httpx_client.aclose()
        logger.info("Gracefully shut down HTTP client")


if __name__ == "__main__":
    asyncio.run(main())