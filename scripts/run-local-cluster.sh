#!/usr/bin/env bash
# Start a 3-node kvraft cluster on localhost, in the background.
#   ./scripts/run-local-cluster.sh        start
#   ./scripts/run-local-cluster.sh stop   stop (SIGKILL; the WAL makes that safe)
# Data lives in ./data/node{1,2,3}; delete it for a fresh cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
PEERS=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
if [ "${1:-start}" = "stop" ]; then
  for i in 1 2 3; do
    [ -f data/pid$i ] && kill -9 "$(cat data/pid$i)" 2>/dev/null && rm -f data/pid$i
  done
  echo stopped; exit 0
fi
cargo build --release --bins
mkdir -p data
for i in 1 2 3; do
  ./target/release/kvraft-server --id $i --peers $PEERS \
      --client-listen 127.0.0.1:800$i --data-dir data/node$i \
      > data/node$i.log 2>&1 &
  echo $! > data/pid$i
done
sleep 1
echo "started. try:"
echo "  export KVRAFT_SERVERS=1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003"
echo "  ./target/release/kvraft-cli status"
echo "  ./target/release/kvraft-cli put hello world"
