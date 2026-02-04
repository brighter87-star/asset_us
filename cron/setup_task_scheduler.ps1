# Setup Windows Task Scheduler for daily_sync.py
# Run as Administrator: powershell -ExecutionPolicy Bypass -File setup_task_scheduler.ps1

$ProjectDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectDir) { $ProjectDir = "C:\asset_us" }

$TaskName = "AssetUS_DailySync"
$PythonPath = "python"  # Or full path like "C:\Python311\python.exe"
$ScriptPath = Join-Path $ProjectDir "cron\daily_sync.py"
$LogDir = Join-Path $ProjectDir "logs"

# Create logs directory
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# Remove existing task if exists
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing task: $TaskName"
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create action (run python script)
# Note: We set TZ environment variable in the script itself, so no need here
$Action = New-ScheduledTaskAction -Execute $PythonPath -Argument $ScriptPath -WorkingDirectory $ProjectDir

# Create trigger: 10:00 AM KST (= 20:00 ET previous day during EST, 21:00 ET during EDT)
# For simplicity, run at 10:00 KST which is roughly after US market close
# Or use multiple triggers for DST handling
$Trigger = New-ScheduledTaskTrigger -Daily -At "10:00AM"

# Only run on weekdays (Mon-Fri) - but Task Scheduler daily trigger runs every day
# We'll handle weekday check in the script or use Weekly trigger
$WeeklyTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "10:00AM"

# Settings
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries

# Register task
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $WeeklyTrigger -Settings $Settings -Description "Daily sync for US stock asset management (runs after US market close)"

Write-Host ""
Write-Host "Task Scheduler job created: $TaskName"
Write-Host "Schedule: 10:00 AM KST (Mon-Fri)"
Write-Host "         = ~20:00 ET (after US market close)"
Write-Host ""
Write-Host "To view/edit: Task Scheduler -> $TaskName"
Write-Host "To run manually: schtasks /run /tn $TaskName"
