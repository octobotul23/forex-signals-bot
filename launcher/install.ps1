# Forex Bot: first-time setup on this PC. Safe to run again.
#   1. creates the private Python environment (.venv) from requirements-lock.txt, if it is not there yet or the
#      last setup did not finish (.venv\.forexbot-ok is written only once every package is installed)
#   2. puts a "Forex Bot" shortcut in this folder, on the Desktop and in the Start menu
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File launcher\install.ps1 [-NoShortcuts]
#   (-FromApp: the Forex Bot app runs it in a window of its own; one that fails stays open a minute)
param([switch]$NoShortcuts, [switch]$FromApp)
$ErrorActionPreference = 'Stop'
trap {
    Write-Host ''
    Write-Host $_ -ForegroundColor Red
    if ($FromApp) {
        Write-Host 'Forex Bot starts with the packages it has. This window closes in 60 s (or press a key).'
        try { $Host.UI.RawUI.FlushInputBuffer() } catch { }    # a key pressed while pip ran does not count
        $end = (Get-Date).AddSeconds(60)
        while ((Get-Date) -lt $end) {
            try { if ([Console]::KeyAvailable) { break } } catch { }    # no console of its own: the minute is waited
            Start-Sleep -Milliseconds 200
        }
    }
    exit 1
}
$root = Split-Path -Parent $PSScriptRoot
$venv = Join-Path $root '.venv'
$pythonw = Join-Path $venv 'Scripts\pythonw.exe'
$python = Join-Path $venv 'Scripts\python.exe'
$lock = Join-Path $root 'requirements-lock.txt'
$marker = Join-Path $venv '.forexbot-ok'
$want = (Get-FileHash $lock -Algorithm SHA256).Hash

if (-not (Test-Path $pythonw)) {
    Write-Host 'Setting up the Python environment (first time only, a few minutes)...'
    if (Get-Command py -ErrorAction SilentlyContinue) { & py -3.12 -m venv $venv }
    else { & python -m venv $venv }
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $pythonw)) { throw 'Could not create .venv: install Python 3.12 from python.org first.' }
}
$have = if (Test-Path $marker) { (Get-Content $marker -Raw).Trim() } else { '' }
if ($have -ne $want) {
    Write-Host 'Installing the packages (a few minutes)...'
    & $python -m ensurepip --upgrade    # a .venv made without pip (or with a broken one) gets it back first
    & $python -m pip install --upgrade pip
    & $python -m pip install -r $lock
    if ($LASTEXITCODE -ne 0) { throw 'pip install failed: see the messages above, then run Forex Bot again.' }
    Set-Content -Path $marker -Value $want -Encoding ascii
}

if (-not $NoShortcuts) {
    $shell = New-Object -ComObject WScript.Shell
    $places = @($root, [Environment]::GetFolderPath('Desktop'), [Environment]::GetFolderPath('Programs'))
    foreach ($place in $places) {
        $link = $shell.CreateShortcut((Join-Path $place 'Forex Bot.lnk'))
        $link.TargetPath = $pythonw
        $link.Arguments = '"' + (Join-Path $root 'launcher\forexbot.py') + '"'
        $link.WorkingDirectory = $root
        $link.IconLocation = (Join-Path $root 'launcher\forexbot.ico') + ',0'
        $link.Description = 'Forex Bot: platform + MetaTrader 5 connector'
        $link.Save()
        Write-Host "Shortcut: $(Join-Path $place 'Forex Bot.lnk')"
    }
}
Write-Host 'Forex Bot is ready.'
