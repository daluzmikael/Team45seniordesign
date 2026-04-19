# Run in Cursor terminal:  .\RUN_IN_CURSOR_TERMINAL.ps1
# Optional:  .\RUN_IN_CURSOR_TERMINAL.ps1 -RepoUrl "https://github.com/YOU/REPO.git"

param(
    [string]$RepoUrl = ""
)

$ErrorActionPreference = "Stop"
$gh = Join-Path ${env:ProgramFiles} "GitHub CLI\gh.exe"

Set-Location $PSScriptRoot
Write-Host "`n=== 1) Project folder ===" -ForegroundColor Cyan
Write-Host (Get-Location)

Write-Host "`n=== 2) Log in to GitHub (browser or device — complete in browser) ===" -ForegroundColor Cyan
if (-not (Test-Path $gh)) {
    Write-Error "Install GitHub CLI: winget install GitHub.cli"
}
& $gh auth login --hostname github.com --git-protocol https

Write-Host "`n=== 3) Verify login ===" -ForegroundColor Cyan
& $gh auth status

Write-Host "`n=== 4) Configure git to use gh for HTTPS ===" -ForegroundColor Cyan
& $gh auth setup-git

if (-not $RepoUrl) {
    $RepoUrl = Read-Host "`nPaste your GitHub repo HTTPS URL (e.g. https://github.com/you/repo.git)"
}

if ($RepoUrl -notmatch "^https://github\.com/") {
    Write-Error "URL must look like https://github.com/USER/REPO.git"
}

Write-Host "`n=== 5) Set git remote 'origin' ===" -ForegroundColor Cyan
$hasOrigin = git remote get-url origin 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "Updating existing origin -> $RepoUrl"
    git remote set-url origin $RepoUrl
} else {
    git remote add origin $RepoUrl
}
git remote -v

Write-Host "`n=== 6) Push main, then feature branch ===" -ForegroundColor Cyan
git checkout main
git push -u origin main
git checkout feature/sentry-updates
git push -u origin feature/sentry-updates

Write-Host "`n=== 7) Open pull request (main <- feature/sentry-updates) ===" -ForegroundColor Cyan
& $gh pr create --base main --head feature/sentry-updates --title "feat: SQL normalizers, UTF-8, frontend lint" --body @"
## Summary
- Backend: SQL normalizers, UTF-8 stdio, logging fixes
- Frontend: ESLint / hooks / types, chat errors
- Docs: README, PR script

Merge when ready.
"@

Write-Host "`n=== Done ===" -ForegroundColor Green
