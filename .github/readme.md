# CI maintenance

## Ownership

- `workflows/`: triggers, job names, runner/version matrices, permissions, and failure policy.
- `actions/setup-python/`: pinned Python and uv setup, cache settings, selected interpreter output.
- `actions/install-dependencies/`: dependency installation and optional latest-Python wheel probe.
- `actions/python-check/`: shared setup, dependency gating, protobuf generation, and suite execution.
- `scripts/install_dependencies.py`: one dependency command for both probing and installing.
- `scripts/run_checks.py`: core unit, example unit, integration, combined, and lint commands.
- `scripts/report_probe.py`: cross-platform warnings and job summaries.
- `scripts/create_smoke_environment.sh`: isolated installed-package smoke environment.

## Changing checks

Use `python-check` after checkout. Set `python-version` and `suite`; override
`requirements` for weekly gRPC checks or `extra-requirements` for development tools.
Example dependencies are automatically added for the `examples` suite. Custom
protobuf bindings are generated for integration and lint suites; `include-custom`
also enables them for unit-only ARM probes.

Keep core and example tests in separate jobs. Keep matrices and job IDs in the
calling workflows so supported platforms and required checks remain visible.
Publishing uses only shared setup, with caching disabled; smoke checks use shared
dependency installation but retain a separate environment outside the checkout.

## Latest Python policy

Floating `3.x` jobs must remain non-blocking. The dependency action probes the
exact installation requirements with binary-only gRPC dependencies. An unavailable
wheel or dependency resolution failure emits a warning and skips installation,
generation, and checks. Pinned Python jobs install directly and remain mandatory.
Pass the selected interpreter explicitly through `python-path`; subsequent uv
commands use that same interpreter through `UV_PYTHON`.

## Local validation

- Workflow syntax: `uvx --from actionlint-py actionlint -ignore '^specifying action \x22\$/\.github/actions/[a-z-]+\x22 in invalid format because ref is missing\.'`
- Workflow and composite-action security: `uvx zizmor .github`

Use `$/...` for same-repository action references. The actionlint compatibility
filter excludes only its unsupported-syntax diagnostic for these action paths;
remove the filter when actionlint-py supports self-repository references. Zizmor
still audits all actions without a severity override.

Run full OS/version matrices through GitHub Actions. Local syntax and security
checks do not execute composite actions or prove runner-specific behavior.