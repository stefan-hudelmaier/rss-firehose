# Run all tests
test:
    uv run python -m unittest discover -v

# Run a specific test file
test-file FILE:
    uv run python -m unittest -v {{FILE}}

# Run tests with coverage report
test-coverage:
    uv run python -m coverage run -m unittest discover
    uv run python -m coverage report
    uv run python -m coverage html
