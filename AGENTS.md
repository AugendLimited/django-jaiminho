# Repository guidance

This repository is the Django outbox and message-resilience package.
Follow the [repository README](README.md) for setup and checks.
Run the tests with `tox -e py39`.

At the start of the task:

1. Use `search_knowledge` with `django-jaiminho`, the affected
   component, and the task topic to find relevant approved guidance and
   obtain the published vault revision.
2. Call `get_agent_rules` with profile `shared` at that revision.
   Read the rules using the pinned `get_document` requests it returns,
   following any continuation pages.
3. Read relevant FRD, ADR, and standard sections with `get_document`
   at the same revision.

Architecture, domain, ADR, and standard documents live in the vault.
This repository keeps its README, setup, commands, and instructions that
apply only here.

Treat the MCP as available. If a call fails, stop and report that the vault
could not be read.

If the vault answers and approved guidance is missing or conflicting,
name the gap and continue from the request, the code, and the README.
Follow this repository's review and approval requirements.

A change to a vault document is a pull request under `knowledge/` in
`augend-engineering-knowledge`. Leave that pull request for a person to
approve.
