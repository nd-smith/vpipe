# PowerShell script to get Windows proxy configuration
# Run this in PowerShell: ./get-proxy.ps1

Write-Host "=== Windows Proxy Configuration ===" -ForegroundColor Green

# Get proxy from Internet Settings
$proxy = (Get-ItemProperty -Path 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings').ProxyServer

if ($proxy) {
    Write-Host "`nProxy Server: $proxy" -ForegroundColor Yellow
    
    # Format for .env file
    if ($proxy -notmatch '^https?://') {
        $proxy = "http://$proxy"
    }
    
    Write-Host "`n=== Add to .env file ===" -ForegroundColor Green
    Write-Host "HTTP_PROXY=$proxy"
    Write-Host "HTTPS_PROXY=$proxy"
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
