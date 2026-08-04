---
name: Feature Suggestion / Merge Request
about: Request to merge a new feature or bug fix
title: ''
labels: ''
reviewers: ''
---

## Related Problem
Describe the issue this pull request addresses.
Example: *This pull request changes [...] because [...]*

## Proposed Solution
Clearly explain the changes introduced in this pull request.

## [Optional] Alternatives Considered
Mention any alternative approaches or solutions you explored.

## Checklist
- [ ] Tests added or updated for the change
- [ ] `unittest discover` passes locally
- [ ] Proto changes regenerated via `python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. --pyi_out=. pookiepy/message.proto`
- [ ] No hardcoded credentials or secrets

## Additional Context
Include any relevant screenshots, links, or context that helps reviewers understand the change.
