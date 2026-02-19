import { execSync } from 'child_process';
import logger from '../utils/logger.js';

/**
 * Get the currently active window info (app name + window title).
 * Cross-platform: Mac (osascript) and Windows (powershell).
 */
export function getActiveWindow() {
  try {
    if (process.platform === 'darwin') {
      return getActiveWindowMac();
    } else if (process.platform === 'win32') {
      return getActiveWindowWindows();
    } else {
      logger.warn('Unsupported platform for active window detection', { platform: process.platform });
      return { appName: 'unknown', windowTitle: 'unknown' };
    }
  } catch (error) {
    logger.error('Failed to get active window', { error: error.message });
    return { appName: 'unknown', windowTitle: 'unknown' };
  }
}

function getActiveWindowMac() {
  // Use separate -e flags for each line to avoid shell escaping issues
  const result = execSync(
    'osascript ' +
    '-e \'tell application "System Events"\' ' +
    '-e \'set frontApp to name of first application process whose frontmost is true\' ' +
    '-e \'end tell\' ' +
    '-e \'tell application frontApp\' ' +
    '-e \'try\' ' +
    '-e \'set windowTitle to name of front window\' ' +
    '-e \'on error\' ' +
    '-e \'set windowTitle to "No Window"\' ' +
    '-e \'end try\' ' +
    '-e \'end tell\' ' +
    '-e \'return frontApp & "|" & windowTitle\'',
    { timeout: 3000, encoding: 'utf-8' }
  ).trim();

  const separatorIndex = result.indexOf('|');
  if (separatorIndex === -1) {
    return { appName: result || 'unknown', windowTitle: 'unknown' };
  }
  return {
    appName: result.substring(0, separatorIndex) || 'unknown',
    windowTitle: result.substring(separatorIndex + 1) || 'unknown'
  };
}

function getActiveWindowWindows() {
  const script = `
    Add-Type @"
    using System;
    using System.Runtime.InteropServices;
    using System.Text;
    using System.Diagnostics;
    public class WindowHelper {
      [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
      [DllImport("user32.dll")] public static extern int GetWindowText(IntPtr hWnd, StringBuilder text, int count);
      [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint processId);
    }
"@
    $hwnd = [WindowHelper]::GetForegroundWindow()
    $sb = New-Object System.Text.StringBuilder 256
    [WindowHelper]::GetWindowText($hwnd, $sb, 256) | Out-Null
    $title = $sb.ToString()
    $pid = 0
    [WindowHelper]::GetWindowThreadProcessId($hwnd, [ref]$pid) | Out-Null
    $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
    $appName = if ($proc) { $proc.ProcessName } else { "unknown" }
    Write-Output "$appName|$title"
  `;

  const result = execSync(`powershell -Command "${script.replace(/"/g, '\\"')}"`, {
    timeout: 3000,
    encoding: 'utf-8'
  }).trim();

  const [appName, windowTitle] = result.split('|');
  return {
    appName: appName || 'unknown',
    windowTitle: windowTitle || 'unknown'
  };
}

/**
 * Check if the system is idle (screen locked or no recent input).
 * Mac only for now — uses HIDIdleTime.
 */
export function isSystemIdle(idleThresholdMs = 300000) {
  try {
    if (process.platform === 'darwin') {
      const idleTime = execSync(
        'ioreg -c IOHIDSystem | awk \'/HIDIdleTime/ {print $NF; exit}\'',
        { timeout: 2000, encoding: 'utf-8' }
      ).trim();
      const idleMs = parseInt(idleTime, 10) / 1000000; // nanoseconds to ms
      return idleMs > idleThresholdMs;
    }
    // Windows: could use GetLastInputInfo, but skip for now
    return false;
  } catch (error) {
    logger.error('Failed to check idle state', { error: error.message });
    return false;
  }
}
