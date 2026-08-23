$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$tracked = git -C $root ls-files
$forbiddenPaths = $tracked | Where-Object {
    $_ -match '(^|/)(private|user_data|secrets|data)/' -or
    $_ -match 'CK_Trend' -or
    ($_ -match '(^|/)config.*\.json$' -and $_ -ne 'config.example.json')
}

if ($forbiddenPaths) {
    throw "Private paths are tracked:`n$($forbiddenPaths -join "`n")"
}

$patterns = @(
    'api[_-]?key\s*[=:]\s*["''][^"'']{8,}',
    'secret\s*[=:]\s*["''][^"'']{8,}',
    'telegram.*token\s*[=:]',
    '-----BEGIN .*PRIVATE KEY-----'
)

foreach ($pattern in $patterns) {
    $matches = git -C $root grep -n -I -E $pattern -- . 2>$null
    if ($LASTEXITCODE -eq 0 -and $matches) {
        throw "Possible credential matched '$pattern':`n$matches"
    }
}

Write-Output 'Privacy check passed: no private strategy/config paths or credential patterns are tracked.'
exit 0
