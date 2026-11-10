#!/usr/bin/env bash
# Starts MongoDB, HBase and the HBase Thrift server (run after every reboot).
# Usage:  bash setup/start_services.sh
set -uo pipefail
HBASE_HOME="${HBASE_HOME:-$HOME/hbase}"
export PATH="$HOME/mongodb/bin:$PATH"

echo "== MongoDB"
RAM_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
CACHE=1; [ "$RAM_MB" -lt 6000 ] && CACHE=0.25          # small laptop: 256 MB cache
if pgrep -x mongod >/dev/null; then
  echo "already running"
else
  # started by hand (not systemd) so the cache size can be limited
  command -v systemctl >/dev/null && sudo systemctl stop mongod 2>/dev/null
  mkdir -p "$HOME/mongo-data"
  mongod --dbpath "$HOME/mongo-data" --wiredTigerCacheSizeGB "$CACHE" --fork --logpath "$HOME/mongod.log" \
    && echo "started (cache ${CACHE} GB)"
fi

echo "== HBase"
if pgrep -f HMaster >/dev/null; then
  echo "already running"
else
  "$HBASE_HOME/bin/start-hbase.sh"
fi

echo "== HBase Thrift server (port 9090)"
if pgrep -f ThriftServer >/dev/null; then
  echo "already running"
else
  "$HBASE_HOME/bin/hbase-daemon.sh" start thrift
fi

echo "Waiting for HBase to be ready..."
for i in $(seq 1 30); do
  if echo "status" | "$HBASE_HOME/bin/hbase" shell -n >/dev/null 2>&1; then
    echo "HBase is ready.  Web UI: http://localhost:16010"; exit 0
  fi
  sleep 5
done
echo "HBase is still starting - check logs in $HBASE_HOME/logs if the next step fails."
