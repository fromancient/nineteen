#!/bin/bash

# THIS FILE CONTAINS THE STEPS NEEDED TO AUTOMATICALLY UPDATE THE REPO ON A TAG CHANGE
# THIS FILE ITSELF MAY CHANGE FROM UPDATE TO UPDATE, SO WE CAN DYNAMICALLY FIX ANY ISSUES
source $HOME/.venv/bin/activate

if [ -f .vali.env ]; then
    export $(cat .vali.env | grep -v '^#' | xargs)
else
    echo "Error: .vali.env file not found"
fi


# docker compose --env-file .vali.env -f docker-compose.yml run -e LOCALHOST=false --entrypoint "python src/migration.py" control_node
# ./utils/launch_validator.sh
# task auditor-autoupdates

sh -c '[ -f .auditor.env ] || cp .vali.env .auditor.env'

if [ -z "${IS_AUDITOR}" ] || [ "${IS_AUDITOR}" = "1" ]; then
    docker compose --env-file .vali.env --profile entry_node_profile down --remove-orphans
    docker image prune -a -f --filter "until=168h"
    
    pip install -e .
    pip install -r validator/control_node/requirements.txt

    source $HOME/.venv/bin/activate

    pm2 delete auditor || true
    pm2 start auditor-ecosystem.config.js
else
    ./utils/launch_validator.sh
fi

echo "Autoupdate steps complete :-)"
