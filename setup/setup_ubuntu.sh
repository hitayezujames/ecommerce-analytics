#!/usr/bin/env bash
# =============================================================================
# One-time installer for Ubuntu 22.04 / 24.04 (also inside Windows WSL).
# Installs: Java 17, Python venv + project libraries (PySpark, pymongo, happybase...),
#           MongoDB 8.0, Apache HBase (standalone) with the Thrift server.
# Usage (from the project folder):   bash setup/setup_ubuntu.sh
# Takes ~10-15 minutes. You will be asked for your Ubuntu password (sudo).
# =============================================================================
set -euo pipefail
PROJECT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$HOME/bda-venv"
HBASE_DIR="$HOME/hbase"
CODENAME="$(. /etc/os-release && echo "$VERSION_CODENAME")"
echo "Project: $PROJECT   Ubuntu: $CODENAME"

echo "== 1/5  System packages (Java 17, Python, build tools)"
sudo apt-get update -y
sudo apt-get install -y curl wget gnupg ca-certificates unzip build-essential
sudo apt-get install -y openjdk-17-jdk-headless || sudo apt-get install -y openjdk-21-jdk-headless
JAVA_HOME_DIR="$(ls -d /usr/lib/jvm/java-17-openjdk-* 2>/dev/null | head -1)"   # Spark 3.5 + HBase run on Java 17
[ -n "$JAVA_HOME_DIR" ] || JAVA_HOME_DIR="$(ls -d /usr/lib/jvm/java-21-openjdk-* | head -1)"
echo "Java 17: $JAVA_HOME_DIR"

echo "== 2/5  Python 3.11 virtual environment + libraries"
# Spark 3.5 supports Python <= 3.12, but new Ubuntu ships 3.13/3.14 -> install a private Python 3.11
# with 'uv' (a fast Python installer). Temporary build files go to the disk, not to /tmp (RAM).
export TMPDIR="$HOME/.cache/bda-tmp"; mkdir -p "$TMPDIR"
if ! command -v uv >/dev/null && [ ! -x "$HOME/.local/bin/uv" ]; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
UV="$(command -v uv || echo "$HOME/.local/bin/uv")"
if [ -x "$VENV/bin/python" ] && ! "$VENV/bin/python" -c 'import sys; exit(sys.version_info[:2] != (3, 11))'; then
  echo "Removing old virtual environment (wrong Python version)"; rm -rf "$VENV"
fi
[ -x "$VENV/bin/python" ] || "$UV" venv --python 3.11 "$VENV"
"$UV" pip install --python "$VENV/bin/python" -r "$PROJECT/requirements.txt"
"$VENV/bin/python" -c "import pyspark, pymongo, happybase, faker; print('Python libraries OK - PySpark', pyspark.__version__)"
rm -rf "$TMPDIR"

echo "== 3/5  MongoDB 8.0"
if ! command -v mongod >/dev/null && [ ! -x "$HOME/mongodb/bin/mongod" ]; then
  if [ "$CODENAME" = "noble" ] || [ "$CODENAME" = "jammy" ]; then
    curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
        sudo gpg --yes -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
    echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu ${CODENAME}/mongodb-org/8.0 multiverse" | \
        sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
    sudo apt-get update -y
    sudo apt-get install -y mongodb-org
  else
    # Newer Ubuntu without an official MongoDB repo yet: use the generic Ubuntu 24.04 build
    sudo apt-get install -y libcurl4t64 2>/dev/null || sudo apt-get install -y libcurl4 || true
    MV=8.0.4
    wget -q --show-progress -O "$HOME/mongodb.tgz" \
      "https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2404-${MV}.tgz"
    tar -xzf "$HOME/mongodb.tgz" -C "$HOME" && rm "$HOME/mongodb.tgz"
    mv "$HOME/mongodb-linux-x86_64-ubuntu2404-${MV}" "$HOME/mongodb"
  fi
fi
export PATH="$HOME/mongodb/bin:$PATH"
mongod --version | head -1

echo "== 4/5  Apache HBase (standalone)"
if [ ! -d "$HBASE_DIR" ]; then
  FILE="$(curl -fsSL https://downloads.apache.org/hbase/stable/ | grep -o 'hbase-[0-9.]*-bin.tar.gz' | head -1)"
  [ -n "$FILE" ] || { echo "Could not find HBase on downloads.apache.org"; exit 1; }
  VER="${FILE#hbase-}"; VER="${VER%-bin.tar.gz}"
  echo "Downloading $FILE ..."
  wget -q --show-progress -O "/tmp/$FILE" "https://downloads.apache.org/hbase/stable/$FILE" \
    || wget -q --show-progress -O "/tmp/$FILE" "https://archive.apache.org/dist/hbase/$VER/$FILE"
  tar -xzf "/tmp/$FILE" -C "$HOME"
  mv "$HOME/hbase-$VER" "$HBASE_DIR"
fi
mkdir -p "$HOME/hbase-data" "$HOME/zookeeper-data"
cat > "$HBASE_DIR/conf/hbase-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>hbase.cluster.distributed</name><value>false</value></property>
  <property><name>hbase.rootdir</name><value>file://$HOME/hbase-data</value></property>
  <property><name>hbase.zookeeper.property.dataDir</name><value>$HOME/zookeeper-data</value></property>
  <property><name>hbase.unsafe.stream.capability.enforce</name><value>false</value></property>
  <property><name>hbase.tmp.dir</name><value>$HOME/hbase-tmp</value></property>
  <property><name>zookeeper.session.timeout</name><value>180000</value></property>
  <property><name>hbase.zookeeper.property.tickTime</name><value>6000</value></property>
</configuration>
EOF
grep -q "^export JAVA_HOME=" "$HBASE_DIR/conf/hbase-env.sh" || \
  echo "export JAVA_HOME=$JAVA_HOME_DIR" >> "$HBASE_DIR/conf/hbase-env.sh"
RAM_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
HEAP=1g; [ "$RAM_MB" -lt 6000 ] && HEAP=512m          # small laptop: keep HBase light
sed -i '/^export HBASE_HEAPSIZE=/d' "$HBASE_DIR/conf/hbase-env.sh"
echo "export HBASE_HEAPSIZE=$HEAP" >> "$HBASE_DIR/conf/hbase-env.sh"
# Ignore any Hadoop found on the PATH (e.g. a Windows Hadoop visible from WSL) - use HBase's own jars
grep -q "HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP" "$HBASE_DIR/conf/hbase-env.sh" || \
  echo "export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true" >> "$HBASE_DIR/conf/hbase-env.sh"
echo "HBase heap: $HEAP (RAM: ${RAM_MB} MB)"

echo "== 5/5  Environment variables (~/.bashrc)"
if ! grep -q "# bda-project" "$HOME/.bashrc"; then
  cat >> "$HOME/.bashrc" <<EOF
# bda-project
export JAVA_HOME=$JAVA_HOME_DIR
export HBASE_HOME=$HBASE_DIR
export PATH=\$HBASE_HOME/bin:\$HOME/mongodb/bin:\$HOME/.local/bin:\$PATH
export PYSPARK_PYTHON=$VENV/bin/python
source $VENV/bin/activate
EOF
fi

echo
echo "Setup finished. Next:"
echo "  1) close and reopen the terminal (or run: source ~/.bashrc)"
echo "  2) bash setup/start_services.sh"
echo "  3) python run_all.py"
