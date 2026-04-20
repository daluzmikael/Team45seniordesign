# Run once per clone: registers .githooks so pushes to `main` are blocked at the Git client.
Set-Location $PSScriptRoot
git config core.hooksPath .githooks
Write-Host "Hooks path set to .githooks — pre-push will refuse pushes to branch 'main'."
