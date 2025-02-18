from httpx import Client
from core import constants as ccst
from core.models import config_models as cmodels
from fiber.logging_utils import get_logger
import os
from urllib.parse import urlparse
import aiohttp


logger = get_logger(__name__)
from urllib.parse import urlparse
import os
import aiohttp
import asyncio
from typing import Optional
from aiohttp import ClientTimeout
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

async def download_s3_file(file_url: str, timeout: int = 600) -> str:
    """
    Download a file from S3 with extended timeout and robust error handling.
    
    Args:
        file_url: The URL to download from
        timeout: Timeout in seconds (default 600 seconds = 10 minutes)
    
    Returns:
        str: Path to the downloaded file
    
    Raises:
        Exception: If download fails after retries
    """
    parsed_url = urlparse(file_url)
    file_name = os.path.basename(parsed_url.path)
    local_file_path = os.path.join("/tmp", file_name)

    timeout_config = ClientTimeout(
        total=timeout,
        connect=60,
        sock_read=timeout,
        sock_connect=60
    )

    max_retries = 3
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession(timeout=timeout_config) as session:
                async with session.get(file_url) as response:
                    if response.status == 200:
                        # stream the file content instead of loading it all into memory
                        with open(local_file_path, "wb") as f:
                            chunk_size = 1024 * 1024  # 1MB chunks
                            while True:
                                chunk = await response.content.read(chunk_size)
                                if not chunk:
                                    break
                                f.write(chunk)
                        
                        logger.info(f"Successfully downloaded file to {local_file_path}")
                        return local_file_path
                    else:
                        error_msg = f"Failed to download file: HTTP {response.status}"
                        logger.error(error_msg)
                        raise Exception(error_msg)

        except asyncio.TimeoutError:
            logger.warning(f"Timeout on attempt {attempt + 1}/{max_retries}")
            if attempt == max_retries - 1:
                raise Exception(f"Failed to download file after {max_retries} attempts due to timeout")
            
        except Exception as e:
            logger.warning(f"Error on attempt {attempt + 1}/{max_retries}: {str(e)}")
            if attempt == max_retries - 1:
                raise Exception(f"Failed to download file after {max_retries} attempts: {str(e)}")
            
        if os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
            except Exception as e:
                logger.warning(f"Failed to clean up partial file: {str(e)}")

        if attempt < max_retries - 1:
            retry_wait = retry_delay * (2 ** attempt)  # Exponential backoff
            logger.info(f"Waiting {retry_wait} seconds before retry...")
            await asyncio.sleep(retry_wait)

    raise Exception("Failed to download file after all retries")

def fetch_voted_weights() -> dict[str, float]:
    url = ccst.BASE_TAOVISION_API_URL + "v1/weights"
    with Client() as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


def get_updated_task_config_with_voted_weights(
    task_configs: dict[str, cmodels.FullTaskConfig],
) -> dict[str, cmodels.FullTaskConfig]:
    """
    Replace all default task configs weights with the voted weights to follow consensus 
    [you should normalise after this]
    """
    try:
        # Get the voted weights (those decided by other validators)

        voted_weights = fetch_voted_weights()
        logger.debug(f"Voted weights: {voted_weights}")

        weights_for_tasks_we_support = {
            task_name: weight for task_name, weight in voted_weights.items() if weight > 0 and task_name in task_configs
        }

        if not voted_weights:
            return task_configs

        # Normalise all weights
        for task_name, task_config in task_configs.items():
            logger.debug(f"Task name: {task_name}, weight: {weights_for_tasks_we_support.get(task_name, 0)}")
            task_config.weight = weights_for_tasks_we_support.get(task_name, task_config.weight)
            if task_config.weight == 0:
                task_config.enabled = False

        return task_configs
    except Exception as e:
        logger.error(f"Error when updating task config with voted weights: {e}")


def normalise_task_config_weights(task_configs: dict[str, cmodels.FullTaskConfig]) -> dict[str, cmodels.FullTaskConfig]:
    total_weight = sum(task_config.weight for task_config in task_configs.values())
    if total_weight <= 0:
        raise ValueError(f"Total weight is {total_weight} for all tasks - how? It should be >0")
    for task_config in task_configs.values():
        task_config.weight /= total_weight
    return task_configs