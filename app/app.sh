#!/bin/bash
set -euo pipefail

cd /app

service ssh restart || true

bash /app/start-services.sh

# Environment for indexer
rm -rf /app/.venv-indexer
python3 -m venv /app/.venv-indexer
source /app/.venv-indexer/bin/activate

pip install --upgrade pip
pip install -r /app/requirements-indexer.txt
pip install venv-pack

rm -f /app/.venv-indexer.tar.gz
venv-pack -p /app/.venv-indexer -o /app/.venv-indexer.tar.gz

# Run preparation + indexing with indexer env
bash /app/prepare_data.sh
bash /app/index.sh

deactivate

# Environment for query
rm -rf /app/.venv-query
python3 -m venv /app/.venv-query
source /app/.venv-query/bin/activate

pip install --upgrade pip
pip install -r /app/requirements-query.txt
pip install venv-pack

rm -f /app/.venv-query.tar.gz
venv-pack -p /app/.venv-query -o /app/.venv-query.tar.gz

deactivate

# Upload lightweight query env to HDFS once
hdfs dfs -mkdir -p /user/root || true
hdfs dfs -put -f /app/.venv-query.tar.gz /user/root/.venv-query.tar.gz

echo "Pipeline finished successfully. Keeping container alive."
tail -f /dev/null