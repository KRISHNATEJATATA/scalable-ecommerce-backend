<#
.SYNOPSIS
    Launch GitHub Copilot CLI in autopilot mode with a scoped permission allowlist.

.DESCRIPTION
    Autopilot is non-interactive: it never shows approval prompts, so any tool call
    not covered by an allow rule is auto-denied. This script pre-approves the tools,
    paths and URLs this repository actually needs so autopilot can run end-to-end,
    while keeping destructive operations (`git push`, `rm`) denied.

    Deny rules always take precedence over allow rules.

.PARAMETER Prompt
    Optional prompt to run non-interactively (passed to `copilot -p`).

.PARAMETER Yolo
    Bypass the scoped allowlist and grant all permissions (--yolo). Use with care.

.EXAMPLE
    .\scripts\copilot-auto.ps1

.EXAMPLE
    .\scripts\copilot-auto.ps1 -Prompt "Run the test suite and fix any failures"
#>
[CmdletBinding()]
param(
    [string]$Prompt,
    [switch]$Yolo
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not (Get-Command copilot -ErrorAction SilentlyContinue)) {
    throw "The 'copilot' CLI was not found on PATH. Install it first: npm install -g @github/copilot"
}

$copilotArgs = @('--autopilot')

if ($Yolo) {
    $copilotArgs += '--yolo'
}
else {
    # Toolchain used by the Makefile, pytest, Alembic and the docker-compose stack.
    $allowTools = @(
        'shell(python)'
        'shell(py)'
        'shell(pytest)'
        'shell(pip)'
        'shell(ruff)'
        'shell(alembic)'
        'shell(lint-imports)'
        'shell(uvicorn)'
        'shell(make)'
        'shell(docker)'
        'shell(docker-compose)'
        'shell(npm:*)'
        'shell(npx)'
        'shell(git:*)'
        'shell(gh:*)'
        'shell(pre-commit)'
        'write'
    )

    # Never let an unattended run publish commits or delete files.
    $denyTools = @(
        'shell(git push)'
        'shell(git reset)'
        'shell(rm)'
        'shell(Remove-Item)'
        'write(.env)'
    )

    # Package indexes and the docs/APIs the agent legitimately reads.
    $allowUrls = @(
        'pypi.org'
        'files.pythonhosted.org'
        'registry.npmjs.org'
        'github.com'
        'api.github.com'
        'docs.github.com'
    )

    foreach ($t in $allowTools) { $copilotArgs += "--allow-tool=$t" }
    foreach ($t in $denyTools) { $copilotArgs += "--deny-tool=$t" }
    foreach ($u in $allowUrls) { $copilotArgs += "--allow-url=$u" }
}

if ($Prompt) {
    $copilotArgs += @('-p', $Prompt)
}

Write-Host "Starting Copilot CLI in autopilot mode from $repoRoot" -ForegroundColor Cyan
& copilot @copilotArgs
exit $LASTEXITCODE
