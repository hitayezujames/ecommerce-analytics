# ONE command that checks ALL the software needed by the project, from PowerShell.
#   powershell -ExecutionPolicy Bypass -File setup\check_windows.ps1
# Part 1 checks Windows (RAM, disk, virtualization, WSL/Ubuntu).
# Part 2 runs setup/check_setup.sh inside Ubuntu (Java, Python libraries, MongoDB, HBase, Spark).
function Ok($m)       { Write-Host "  [OK]      $m" -ForegroundColor Green }
function Bad($m, $f)  { Write-Host "  [MISSING] $m" -ForegroundColor Red; Write-Host "            fix: $f"; $script:fail++ }
$fail = 0

Write-Host "=== PART 1: Windows" -ForegroundColor Cyan
$os = Get-CimInstance Win32_OperatingSystem
Ok "$($os.Caption) (build $($os.BuildNumber))"
$ram = [math]::Round($os.TotalVisibleMemorySize / 1MB, 1)
if ($ram -ge 8) { Ok "RAM: $ram GB" } else { Bad "RAM: $ram GB (8 GB+ recommended)" "run the project with: python run_all.py --scale 0.05" }
$free = [math]::Round((Get-PSDrive C).Free / 1GB, 1)
if ($free -ge 10) { Ok "Free space on C: $free GB" } else { Bad "Free space on C: $free GB" "free at least 10 GB" }
try {
  $virt = (Get-CimInstance Win32_Processor | Select-Object -First 1).VirtualizationFirmwareEnabled
  if ($virt -or (Get-CimInstance Win32_ComputerSystem).HypervisorPresent) { Ok "Virtualization enabled" }
  else { Bad "Virtualization disabled" "enable Intel VT-x / AMD-V (SVM) in the BIOS" }
} catch { }

if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
  Bad "WSL not installed" "PowerShell as Administrator: wsl --install -d Ubuntu   (then restart)"
  exit 1
}
$distros = (wsl.exe -l -q) -replace "`0", "" | Where-Object { $_.Trim() -ne "" }
$ubuntu = $distros | Where-Object { $_ -match "Ubuntu" } | Select-Object -First 1
if (-not $ubuntu) {
  Bad "Ubuntu not installed in WSL" "PowerShell as Administrator: wsl --install -d Ubuntu   (then restart)"
  exit 1
}
$ubuntu = $ubuntu.Trim()
Ok "Ubuntu installed in WSL: $ubuntu"
$verbose = ((wsl.exe -l -v) -replace "`0", "") -join "`n"
if ($verbose -match "$([regex]::Escape($ubuntu))\s+\S+\s+2") { Ok "$ubuntu runs on WSL 2" }
else { Bad "$ubuntu runs on WSL 1" "wsl --set-version $ubuntu 2" }

Write-Host ""
Write-Host "=== PART 2: Software inside $ubuntu (Java, Python, MongoDB, HBase, Spark)" -ForegroundColor Cyan
# Prefer the project copy in the Ubuntu home folder; otherwise use this folder via /mnt/c
$homeCopy = wsl.exe -d $ubuntu -- bash -c "test -f ~/ecommerce-analytics/setup/check_setup.sh && echo yes"
if ($homeCopy -eq "yes") {
  $target = '~/ecommerce-analytics/setup/check_setup.sh'
} else {
  $winPath = Join-Path $PSScriptRoot "check_setup.sh"
  $target = (wsl.exe -d $ubuntu -- wslpath -a ($winPath -replace '\\', '/')).Trim()
  Write-Host "  (project not yet copied into Ubuntu - checking from $target)" -ForegroundColor Yellow
}
# bash -i loads ~/.bashrc so JAVA_HOME and the Python environment are active, like in the Ubuntu app
wsl.exe -d $ubuntu -- bash -ic "bash $target"

Write-Host ""
if ($fail -eq 0) { Write-Host "Windows part: all good." -ForegroundColor Green }
else { Write-Host "Windows part: $fail problem(s) - see the 'fix' lines above." -ForegroundColor Red }
