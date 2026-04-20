# Run once per clone: registers .githooks so pushes to `main` are blocked at the Git client.
Set-Location $PSScriptRoot
git config core.hooksPath .githooks
git update-index --chmod=+x .githooks/pre-push 2>$null
Write-Host "Hooks path set to .githooks — pre-push will refuse pushes to branch 'main'."
