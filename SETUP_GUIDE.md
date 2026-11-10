# Installing and running the project on your own computer (no Docker)

What gets installed: **Java 17, Python + PySpark, MongoDB 8, Apache HBase** (standalone) and the
project libraries. Everything is free.

HBase does not run natively on Windows, so on Windows we use **WSL** – the Linux (Ubuntu) that is built
into Windows 10/11. You keep using Windows normally; the project just runs inside an Ubuntu terminal.

---------------------------------------------------------------------------------------------------

## A. Windows 10 / 11

### Check what you already have (any time)
* In **PowerShell** (Windows side): `powershell -ExecutionPolicy Bypass -File setup\check_windows.ps1`
  (or simply `wsl -l -v` – it should list *Ubuntu* with version *2*)
* In the **Ubuntu** window (everything else): `bash setup/check_setup.sh`
  – prints [OK] / [MISSING] for Java, Python libraries, MongoDB, HBase, Spark, with the fix for each.

### Step 1 – Install Ubuntu (WSL) – 5 minutes + 1 restart
1. Click Start, type **PowerShell**, right-click it → **Run as administrator**.
2. Run:
   ```
   wsl --install -d Ubuntu
   ```
3. **Restart** the computer when asked.
4. After the restart an **Ubuntu** window opens (or open "Ubuntu" from the Start menu).
   Choose a **username** and **password** (typing the password shows nothing – that is normal; remember it).

> If you get an error about "virtualization", enable *Virtualization* (Intel VT-x / AMD-V) in the BIOS,
> or in Windows turn on *Virtual Machine Platform* under "Turn Windows features on or off".

### Step 2 – Put the project inside Ubuntu
Download `ecommerce-analytics.zip` to your Windows **Downloads** folder, then in the **Ubuntu** window:
```bash
sudo apt-get update && sudo apt-get install -y unzip
cp /mnt/c/Users/<YOUR_WINDOWS_NAME>/Downloads/ecommerce-analytics.zip ~
cd ~ && unzip ecommerce-analytics.zip && cd ecommerce-analytics
```
(Replace `<YOUR_WINDOWS_NAME>` with your Windows user folder name – run `ls /mnt/c/Users` to see it.)
Keep the project in the Ubuntu home folder (`~`), not on `/mnt/c` – it is much faster.

### Step 3 – Install all the software (one command, ~10–15 min)
```bash
bash setup/setup_ubuntu.sh
```
It asks for your Ubuntu password once. When it ends with *"Setup finished"*:
```bash
source ~/.bashrc
```

### Step 4 – Start MongoDB and HBase
```bash
bash setup/start_services.sh
```
Wait for *"HBase is ready"*. (Do this again every time you restart the computer.)
You can open the HBase web page in your Windows browser: http://localhost:16010

### Step 5 – Run the whole project
```bash
python run_all.py
```
About 10–20 minutes with the default size (`--scale 0.1`: 200k sessions, 50k orders).
At the end you get:
* `data/output/*.csv` – all query results
* `report/figures/*.png` – the charts
* `report/Technical_Report.pdf` – the rebuilt report

To open the results in Windows Explorer: `explorer.exe .`

Go to section C for running parts separately and for troubleshooting.

---------------------------------------------------------------------------------------------------

## B. macOS (Homebrew) or Linux

**Ubuntu / Debian Linux:** follow Windows steps 2–5 in a normal terminal (skip WSL).

**macOS:**
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"   # if no Homebrew
brew install openjdk@17 python@3.11 hbase
brew tap mongodb/brew && brew install mongodb-community
export JAVA_HOME="$(brew --prefix openjdk@17)"          # add these two lines to ~/.zshrc
export PATH="$JAVA_HOME/bin:$PATH"
cd ecommerce-analytics
python3.11 -m venv ~/bda-venv && source ~/bda-venv/bin/activate
pip install -r requirements.txt
brew services start mongodb-community
start-hbase.sh && hbase-daemon.sh start thrift           # HBase + Thrift (port 9090)
python run_all.py
```

---------------------------------------------------------------------------------------------------

## C. Running steps separately

```bash
python run_all.py --scale 0.1              # everything (default)
python run_all.py --scale 1.0              # full brief size: 2M sessions (needs ~16 GB RAM, ~1-2 h)
python run_all.py --from spark             # re-run from a step: generate | mongo | hbase | spark | integration | charts
python run_all.py --skip-db                # only Spark + charts (no MongoDB/HBase needed)
```

Individual scripts (run inside the matching folder):
```bash
cd mongodb && python load_mongodb.py && python aggregations.py --explain
cd hbase   && python create_schema.py && python load_hbase.py && python query_hbase.py --user user_000042
hbase shell hbase/sample_queries.hbase     # HBase shell demo (also: hbase shell hbase/create_schema.hbase)
mongosh                                    # MongoDB shell -> use ecommerce; db.users.findOne()
cd spark   && python 01_clean_normalize.py # then 02_..., 03_..., 04_...
cd integration && python clv_analysis.py --source db && python funnel_analysis.py --source db
```

Stop the databases: `bash setup/stop_services.sh`

## D. Troubleshooting

| Problem | Fix |
|---|---|
| `HBase not reachable` / connection refused on 9090 | `bash setup/start_services.sh` again; wait 1–2 min; check `~/hbase/logs/*.log` |
| HBase stops after a restart of WSL | services do not auto-start – run `bash setup/start_services.sh` |
| `MongoDB not reachable` | `sudo systemctl start mongod` or `mongod --dbpath ~/mongo-data --fork --logpath ~/mongod.log` |
| `JAVA_HOME is not set` / Spark gateway error | `source ~/.bashrc`, then `java -version` must show 17 |
| `python: command not found` | `source ~/bda-venv/bin/activate` |
| Spark runs out of memory | `export SPARK_DRIVER_MEMORY=2g` or use `--scale 0.05` |
| WSL is slow / uses too much RAM | create `C:\Users\<you>\.wslconfig` with `[wsl2]` and `memory=6GB`, then `wsl --shutdown` in PowerShell |
| Want a clean re-run | `python run_all.py` drops and reloads the MongoDB collections and HBase tables automatically |
