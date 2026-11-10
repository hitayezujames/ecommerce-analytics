#!/usr/bin/env bash
# Checks that every piece of software the project needs is installed and running.
# Usage (inside the Ubuntu / WSL terminal, from the project folder):
#     bash setup/check_setup.sh
GREEN="\e[32m"; RED="\e[31m"; YELLOW="\e[33m"; NC="\e[0m"
PASS=0; FAIL=0
ok()   { echo -e "  ${GREEN}[OK]${NC}      $1"; PASS=$((PASS+1)); }
bad()  { echo -e "  ${RED}[MISSING]${NC} $1"; echo -e "            fix: $2"; FAIL=$((FAIL+1)); }
warn() { echo -e "  ${YELLOW}[WARN]${NC}    $1"; echo -e "            fix: $2"; }

PROJECT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$HOME/bda-venv"
HBASE_HOME="${HBASE_HOME:-$HOME/hbase}"
export PATH="$HOME/mongodb/bin:$PATH"
PY="$VENV/bin/python"; [ -x "$PY" ] || PY="$(command -v python3)"

echo "=== 1. Operating system"
if grep -qi microsoft /proc/version 2>/dev/null; then ok "Running inside WSL ($(. /etc/os-release; echo "$PRETTY_NAME"))"
else ok "Linux: $(. /etc/os-release 2>/dev/null; echo "${PRETTY_NAME:-unknown}")"; fi
MEM_GB=$(awk '/MemTotal/ {printf "%.1f", $2/1048576}' /proc/meminfo)
if awk "BEGIN{exit !($MEM_GB >= 5)}"; then ok "Memory available to Linux: ${MEM_GB} GB"
else warn "Only ${MEM_GB} GB memory" "use 'python run_all.py --scale 0.05' or raise memory in C:\\Users\\<you>\\.wslconfig"; fi
DISK_GB=$(df -BG "$HOME" | awk 'NR==2 {gsub("G","",$4); print $4}')
if [ "${DISK_GB:-0}" -ge 5 ]; then ok "Free disk space: ${DISK_GB} GB"
else warn "Only ${DISK_GB} GB free disk" "free some space (the 10% dataset needs ~2 GB, full size ~15 GB)"; fi

echo "=== 2. Java (needed by Spark and HBase)"
if command -v java >/dev/null; then
  JV=$(java -version 2>&1 | head -1)
  if echo "$JV" | grep -qE '"(17|11)\.'; then ok "Java: $JV"
  else warn "Java found but not 17/11: $JV" "sudo apt install openjdk-17-jdk ; then: source ~/.bashrc"; fi
else bad "Java" "bash setup/setup_ubuntu.sh   (or: sudo apt install openjdk-17-jdk)"; fi
[ -n "${JAVA_HOME:-}" ] && ok "JAVA_HOME=$JAVA_HOME" || bad "JAVA_HOME not set" "source ~/.bashrc"

echo "=== 3. Python and libraries"
if [ -x "$VENV/bin/python" ]; then ok "Virtual environment: $VENV ($($VENV/bin/python --version))"
else bad "Python virtual environment ($VENV)" "bash setup/setup_ubuntu.sh"; fi
for pkg in pyspark faker numpy pandas pymongo happybase matplotlib reportlab PIL pyarrow; do
  VER=$("$PY" -c "import $pkg; print(getattr($pkg, '__version__', 'installed'))" 2>/dev/null)
  if [ -n "$VER" ]; then ok "python: $pkg $VER"
  else bad "python: $pkg" "source ~/bda-venv/bin/activate && pip install -r requirements.txt"; fi
done

echo "=== 4. MongoDB"
command -v mongod >/dev/null && ok "MongoDB server: $(mongod --version | head -1)" \
  || bad "MongoDB server (mongod)" "bash setup/setup_ubuntu.sh"
command -v mongosh >/dev/null && ok "MongoDB shell: mongosh $(mongosh --version 2>/dev/null)" \
  || warn "mongosh not found (optional)" "sudo apt install mongodb-mongosh"
if "$PY" -c "from pymongo import MongoClient; MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=2000).admin.command('ping')" 2>/dev/null; then
  ok "MongoDB is RUNNING on port 27017"
else bad "MongoDB is not running" "bash setup/start_services.sh"; fi

echo "=== 5. HBase"
[ -x "$HBASE_HOME/bin/hbase" ] && ok "HBase installed: $HBASE_HOME ($(ls "$HBASE_HOME"/lib/hbase-common-*.jar 2>/dev/null | head -1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1))" \
  || bad "HBase ($HBASE_HOME)" "bash setup/setup_ubuntu.sh"
pgrep -f HMaster >/dev/null && ok "HBase master is RUNNING" || bad "HBase is not running" "bash setup/start_services.sh"
pgrep -f ThriftServer >/dev/null && ok "HBase Thrift server is RUNNING" || bad "HBase Thrift server not running" "bash setup/start_services.sh"
if "$PY" -c "import happybase; c=happybase.Connection('localhost', 9090, timeout=5000); c.tables(); c.close()" 2>/dev/null; then
  ok "Python can talk to HBase on port 9090"
else bad "Python cannot connect to HBase on port 9090" "bash setup/start_services.sh, wait 1-2 minutes, run this check again"; fi

echo "=== 6. Spark smoke test (takes ~20 s)"
if "$PY" - <<'EOF' >/dev/null 2>&1
from pyspark.sql import SparkSession
s = SparkSession.builder.master("local[1]").appName("check").getOrCreate()
assert s.range(10).count() == 10
s.stop()
EOF
then ok "Spark starts and runs a job"
else bad "Spark could not start" "check Java 17 + JAVA_HOME (source ~/.bashrc), then: pip install 'pyspark>=3.5.3,<4.0'"; fi

echo "=== 7. Project files"
[ -f "$PROJECT/run_all.py" ] && ok "Project folder: $PROJECT" || bad "run_all.py not found" "run this from inside the ecommerce-analytics folder"
case "$PROJECT" in /mnt/*) warn "Project is on the Windows drive ($PROJECT) - this is slow" "copy it to your Ubuntu home: cp -r \"$PROJECT\" ~ ";; esac
ls "$PROJECT"/data/raw/sessions_*.json >/dev/null 2>&1 && ok "Dataset already generated in data/raw" \
  || echo "  [INFO]    No dataset yet - run_all.py will generate it"

echo
if [ "$FAIL" -eq 0 ]; then echo -e "${GREEN}All $PASS checks passed - you can run:  python run_all.py${NC}"
else echo -e "${RED}$FAIL problem(s) found${NC}, $PASS checks passed. Apply the 'fix' lines above, then run this check again."; fi
