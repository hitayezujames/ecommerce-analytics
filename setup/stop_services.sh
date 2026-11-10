#!/usr/bin/env bash
# Stops the HBase Thrift server, HBase and MongoDB.
HBASE_HOME="${HBASE_HOME:-$HOME/hbase}"
export PATH="$HOME/mongodb/bin:$PATH"
"$HBASE_HOME/bin/hbase-daemon.sh" stop thrift
"$HBASE_HOME/bin/stop-hbase.sh"
pkill -x mongod
echo "All services stopped."
