# Token refresh script for Kafka pipeline
# Refreshes Azure tokens every 45 minutes (tokens expire in ~60 min)
# Usage: .\refresh_tokens.ps1 [-IntervalMinutes 45]

param(
    [int]$IntervalMinutes = 45
)

# Get script directory and set token file path relative to it
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TokenFile = Join-Path $ScriptDir "src\tokens.json"
$IntervalSeconds = $IntervalMinutes * 60

Write-Host "============================================================"
Write-Host "Token Refresh Script"
Write-Host "Token file: $TokenFile"
Write-Host "Refresh interval: $IntervalMinutes minutes"
Write-Host "============================================================"
Write-Host ""

while ($true) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] Refreshing tokens..."

    try {
        # Get storage token
        Write-Host "Getting storage token..."
        $storageToken = az account get-access-token --resource https://storage.azure.com/ --query accessToken -o tsv
        if (-not $storageToken) {
            throw "Failed to get storage token"
        }

        # Get Kusto token
        Write-Host "Getting Kusto token..."
        $kustoToken = az account get-access-token --resource https://kusto.kusto.windows.net --query accessToken -o tsv
        if (-not $kustoToken) {
            throw "Failed to get Kusto token"
        }

        # Write tokens.json (UTF-8 without BOM)
        $tokens = @{
            "https://storage.azure.com/" = $storageToken
            "https://kusto.kusto.windows.net" = $kustoToken
        }
        $json = $tokens | ConvertTo-Json
        [System.IO.File]::WriteAllText($TokenFile, $json, [System.Text.UTF8Encoding]::new($false))

        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        Write-Host "[$timestamp] Tokens refreshed successfully"
        Write-Host "  - Storage token: $($storageToken.Substring(0, 20))..."
        Write-Host "  - Kusto token: $($kustoToken.Substring(0, 20))..."
        Write-Host ""
        Write-Host "Next refresh in $IntervalMinutes minutes. Press Ctrl+C to stop."
        Write-Host ""

        Start-Sleep -Seconds $IntervalSeconds
    }
    catch {
        Write-Host "ERROR: $_" -ForegroundColor Red
        Write-Host "Please run 'az login' and try again."
        exit 1
    }
}
