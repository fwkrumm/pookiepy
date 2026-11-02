NOTE that there are more examples in ../tests/integration. Those are also executed as part of the CI workflow and thus work deterministically but are rather basic.

The examples within THIS directory were tested locally but since they e.g. use specialized APIs (lm studio for example) are not trivial to test end-to-end within the pipeline. Large parts of them are AI generated since I also used it to test how good AI would use the existing mechanics to create working clients/servers.

Example unit tests live separately in `tests/examples/` and run in dedicated "Unit test examples" CI jobs, with external services mocked. Run them from the repository root with `uv run python -m unittest discover -s tests/examples -p "test_*.py"`. Core framework unit tests remain in `tests/`.
