$ErrorActionPreference = 'Stop'
$srcPath = Join-Path -Path $PSScriptRoot -ChildPath '..\src'
$allowedExtensions = @(
    '.cs', '.csproj', '.md', '.json', '.spt', '.txt', '.props', '.targets', '.yml', '.yaml', '.config', '.ts', '.tsx', '.vue', '.sql', '.ini'
)
$replacements = @{
    'â€“' = '-';
    'â€”' = '--';
    'â€˜' = "'";
    'â€™' = "'";
    'â€œ' = '"';
    'â€' = '"';
    'â€ž' = '"';
    'â€¦' = '...';
    'â€¢' = '*';
    'â†’' = '->';
    'Ã¤' = 'ae';
    'Ã„' = 'Ae';
    'Ã¶' = 'oe';
    'Ã–' = 'Oe';
    'Ã¼' = 'ue';
    'Ãœ' = 'Ue';
    'ÃŸ' = 'ss';
    'Ã©' = 'e';
    'Ã€' = 'A';
    'Ã¡' = 'a';
    'Ã¢' = 'a';
    'Ãª' = 'e';
    'Ã¨' = 'e';
    'Ãº' = 'u';
    'Ã³' = 'o';
    'Ã²' = 'o';
    'Ã±' = 'n'
}
$files = Get-ChildItem -Path $srcPath -Recurse -File | Where-Object {
    $ext = $_.Extension
    if ([string]::IsNullOrEmpty($ext)) { return $false }
    $allowedExtensions -contains $ext.ToLowerInvariant()
}
foreach ($file in $files) {
    $content = Get-Content -LiteralPath $file.FullName -Raw
    $updated = $content
    foreach ($key in $replacements.Keys) {
        $updated = $updated.Replace($key, $replacements[$key])
    }
    if ($updated -ne $content) {
        Set-Content -LiteralPath $file.FullName -Value $updated -Encoding UTF8
    }
}
