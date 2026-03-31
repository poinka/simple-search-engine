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


echo "Ensuring YARN worker is ready before demo queries..."
READY=0
for i in $(seq 1 24); do
  if yarn node -list 2>/dev/null | grep -q "RUNNING"; then
    READY=1
    echo "YARN worker is ready."
    break
  fi
  echo "Waiting for YARN worker... ${i}/24"
  sleep 5
done

if [ "$READY" -ne 1 ]; then
  echo "YARN worker did not become ready. Skipping demo queries."
  tail -f /dev/null
fi

echo "Running demo search queries..."

for query in "movie" "hospital" "theologian"; do
  echo
  echo "=================================================="
  echo "Demo query: ${query}"
  echo "=================================================="
  bash /app/search.sh "${query}" || echo "Search failed for query: ${query}"
done

# echo
echo "Pipeline finished successfully. Keeping container alive."
tail -f /dev/null