$output = "all_code_dump.txt"
Clear-Content $output -ErrorAction SilentlyContinue

Get-ChildItem -Path src, tests -Recurse -File |
Where-Object { $_.Extension -in ".rs", ".toml" } |
Sort-Object FullName |
ForEach-Object {
    Add-Content $output "=================================================="
    Add-Content $output "FILE: $($_.FullName)"
    Add-Content $output "=================================================="
    Add-Content $output ""
    Get-Content $_.FullName | Add-Content $output
    Add-Content $output "`n`n"
}

Write-Host "✅ Code dumped into $output"
