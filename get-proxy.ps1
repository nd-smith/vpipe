# PowerShell script to get Windows proxy configuration
# Run this in PowerShell: ./get-proxy.ps1

Write-Host "=== Windows Proxy Configuration ===" -ForegroundColor Green

# Get proxy from Internet Settings
$proxyRaw = (Get-ItemProperty -Path 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings').ProxyServer

if ($proxyRaw) {
    Write-Host "`nRaw Proxy Setting: $proxyRaw" -ForegroundColor Yellow

    $httpProxy = $null
    $httpsProxy = $null

    # Check if it's protocol-specific format (http=host:port;https=host:port)
    if ($proxyRaw -match ';') {
        # Parse protocol-specific proxy settings
        $parts = $proxyRaw -split ';'
        foreach ($part in $parts) {
            if ($part -match '^http=(.+)$') {
                $httpProxy = $matches[1]
            }
            elseif ($part -match '^https=(.+)$') {
                $httpsProxy = $matches[1]
            }
        }

        # Use HTTP proxy for HTTPS if HTTPS not specified
        if (-not $httpsProxy -and $httpProxy) {
            $httpsProxy = $httpProxy
        }
    }
    else {
        # Simple format: just host:port
        $httpProxy = $proxyRaw
        $httpsProxy = $proxyRaw
    }

    # Format URLs properly
    if ($httpProxy -and $httpProxy -notmatch '^https?://') {
        $httpProxy = "http://$httpProxy"
    }
    if ($httpsProxy -and $httpsProxy -notmatch '^https?://') {
        $httpsProxy = "http://$httpsProxy"
    }

    Write-Host "`n=== Add to .env file ===" -ForegroundColor Green
    if ($httpProxy) {
        Write-Host "HTTP_PROXY=$httpProxy"
    }
    if ($httpsProxy) {
        Write-Host "HTTPS_PROXY=$httpsProxy"
    }
    Write-Host "EVENTHOUSE_PROXY_URL=$httpsProxy"
    Write-Host "NO_PROXY=localhost,127.0.0.1,.local"
} else {
    Write-Host "`nNo proxy configured in Windows Internet Settings" -ForegroundColor Red

    # Check system environment variables
    $httpProxy = [System.Environment]::GetEnvironmentVariable("HTTP_PROXY", "User")
    $httpsProxy = [System.Environment]::GetEnvironmentVariable("HTTPS_PROXY", "User")

    if ($httpProxy -or $httpsProxy) {
        Write-Host "`nFound in environment variables:" -ForegroundColor Yellow
        if ($httpProxy) { Write-Host "HTTP_PROXY=$httpProxy" }
        if ($httpsProxy) { Write-Host "HTTPS_PROXY=$httpsProxy" }
    }
}
