"""
run_all.py - runs the whole project end to end (works on Windows, macOS and Linux).

Recommended (only Docker needed):
    docker compose up -d mongodb hbase
    docker compose run --rm app python run_all.py

Without Docker for Python (Python 3.10+, Java 17 and `pip install -r requirements.txt`):
    docker compose up -d mongodb hbase
    python run_all.py

Options:
    --scale 0.1        dataset size (0.1 = 200k sessions / 50k orders; 1.0 = full 2M / 500k)
    --skip-db          Spark + charts only, no MongoDB / HBase needed
    --skip-generate    reuse the data already in data/raw
    --from STEP        start at a step: generate, mongo, hbase, spark, integration, charts
"""
import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
STEPS = ["generate", "mongo", "hbase", "spark", "integration", "charts"]


def run(folder, *args, env=None):
    cmd = [PY, *args]
    print(f"\n>>> [{folder}] {' '.join(args)}", flush=True)
    t0 = time.time()
    res = subprocess.run(cmd, cwd=os.path.join(ROOT, folder), env={**os.environ, **(env or {})})
    if res.returncode != 0:
        sys.exit(f"\nFAILED: {' '.join(args)} (in {folder}/). Fix the error above, then re-run with "
                 f"--from {CURRENT} to continue from this step.")
    print(f"    done in {time.time() - t0:.0f}s", flush=True)


def _ram_gb():
    try:
        with open("/proc/meminfo") as f:
            return int(f.readline().split()[1]) / 1048576
    except OSError:
        return 8.0


def _docker_hbase():
    """True when HBase runs in a Docker container called 'hbase'."""
    try:
        out = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"], capture_output=True, text=True)
        return "hbase" in out.stdout.split()
    except OSError:
        return False


def _wait_for_thrift(timeout=300):
    import socket
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            import happybase
            c = happybase.Connection(os.environ.get("HBASE_HOST", "localhost"),
                                     int(os.environ.get("HBASE_PORT", "9090")), timeout=10000)
            c.tables()
            c.close()
            print("HBase is ready", flush=True)
            return
        except (OSError, socket.error, Exception):  # noqa: BLE001
            time.sleep(10)
    print("HBase did not answer within 5 minutes - continuing anyway", flush=True)


def hbase_service(action):
    """Low-memory helper: pause HBase while Spark runs, start it again afterwards."""
    print(f"Low-memory mode: {action} HBase", flush=True)
    if _docker_hbase():
        subprocess.run(["docker", action, "hbase"])
        if action == "start":
            _wait_for_thrift()
        return
    home = os.environ.get("HBASE_HOME", os.path.expanduser("~/hbase"))
    if not os.path.isdir(home):
        return
    if action == "stop":
        subprocess.run(["bash", os.path.join(home, "bin", "hbase-daemon.sh"), "stop", "thrift"])
        subprocess.run(["bash", os.path.join(home, "bin", "stop-hbase.sh")])
    else:
        subprocess.run(["bash", os.path.join(ROOT, "setup", "start_services.sh")])


def wait_for_mongo():
    from pymongo import MongoClient
    uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    for i in range(30):
        try:
            MongoClient(uri, serverSelectionTimeoutMS=3000).admin.command("ping")
            print(f"MongoDB is up at {uri}")
            return
        except Exception:  # noqa: BLE001
            print(f"waiting for MongoDB at {uri} ... ({i + 1}/30)")
            time.sleep(5)
    sys.exit("MongoDB not reachable. Did you run: docker compose up -d mongodb hbase ?")


def main():
    global CURRENT
    ap = argparse.ArgumentParser()
    ram_gb = _ram_gb()
    default_scale = "0.05" if ram_gb < 6 else "0.1"
    ap.add_argument("--scale", default=os.environ.get("SCALE", default_scale))
    ap.add_argument("--skip-db", action="store_true")
    ap.add_argument("--skip-generate", action="store_true")
    ap.add_argument("--from", dest="start", choices=STEPS, default="generate")
    a = ap.parse_args()
    if ram_gb < 6:
        print(f"Low-memory mode: {ram_gb:.1f} GB RAM -> dataset scale {a.scale}, Spark driver 1 GB")
    todo = STEPS[STEPS.index(a.start):]
    if a.skip_generate and "generate" in todo:
        todo.remove("generate")
    if a.skip_db:
        todo = [s for s in todo if s not in ("mongo", "hbase")]
    t0 = time.time()

    for CURRENT in todo:
        print(f"\n================ STEP: {CURRENT} ================", flush=True)
        if CURRENT == "generate":
            run("data_generation", "dataset_generator.py",
                env={"SCALE": str(a.scale), "OUTPUT_DIR": os.path.join(ROOT, "data", "raw")})
        elif CURRENT == "mongo":
            wait_for_mongo()
            run("mongodb", "load_mongodb.py")
            run("mongodb", "aggregations.py", "--explain")
        elif CURRENT == "hbase":
            if ram_gb < 6 and not os.environ.get("HBASE_MAX_SESSIONS"):
                os.environ["HBASE_MAX_SESSIONS"] = "10000"   # small laptop: HBase holds a 10k-session subset
                print("Low-memory mode: loading a representative subset of 10,000 sessions into HBase")
            run("hbase", "create_schema.py")      # waits for HBase to finish starting
            run("hbase", "load_hbase.py")
            run("hbase", "query_hbase.py")
        elif CURRENT == "spark":
            if ram_gb < 6 and not a.skip_db:
                hbase_service("stop")   # free ~700 MB for Spark; restarted before the integration step
            for job in ("01_clean_normalize.py", "02_recommendations.py",
                        "03_cohort_analysis.py", "04_spark_sql_analytics.py"):
                run("spark", job)
        elif CURRENT == "integration":
            if ram_gb < 6 and not a.skip_db:
                hbase_service("start")
            src = "files" if a.skip_db else "db"
            run("integration", "clv_analysis.py", "--source", src)
            run("integration", "funnel_analysis.py", "--source", src)
        elif CURRENT == "charts":
            run("visualization", "make_charts.py")
            run("visualization", "architecture.py")
            run("visualization", "build_dashboard.py")
            run("report", "build_report.py")

    print(f"\nALL DONE in {(time.time() - t0) / 60:.1f} min.\n"
          f"  results : data/output/*.csv\n  figures : report/figures/*.png\n"
          f"  report  : report/Technical_Report.pdf (update the numbers in report/build_report.py)")


CURRENT = None
if __name__ == "__main__":
    main()
