# Security Policy

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

Report them privately via GitHub:
[Security → Report a vulnerability](https://github.com/zbrainiac-labs/MasterDataManagement/security/advisories/new).
Include the affected component (`bulk/`, `nrt/`, CI), steps to reproduce and the impact you expect.
You will get an acknowledgement within a few working days.

## Scope

This repository is a showcase / reference implementation. It ships synthetic test data only and
local-development defaults (for example the Postgres credentials in `nrt/docker-compose.yml`)
that must not be used in production.

## Supported versions

Only the latest state of `main` (and the latest `ghcr.io/zbrainiac-labs/mdm-nrt:latest` image) is maintained.
