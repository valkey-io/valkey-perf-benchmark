# Integration Tests

This directory contains integration tests that validate benchmark workflows without requiring a real Valkey server.

## Test Coverage

### Git Operations (`test_git_operations.py`)
- Creating and managing temporary git repositories
- Branch creation and checkout
- Commit operations with file changes
- Simulating PR branch structures (baseline vs PR branch)
- Mock Valkey repository structure validation

### Comparison Workflow (`test_comparison_workflow.py`)
- Comparing two metrics files
- RPS-only filtering (used in PR workflow)
- Multiple runs averaging with statistical analysis
- Different configuration handling
- Error handling for missing files
- Metrics file format validation
- PR comment markdown generation

### Benchmark Execution (`test_benchmark_execution.py`)
- Configuration loading and validation
- Mock benchmark binary execution
- Benchmark command building (simple, TLS, CPU pinning)
- Metrics processing from CSV output
- CLI argument validation

### PR Workflow Simulation (`test_pr_workflow.py`)
- End-to-end comparison phase simulation
- Multiple runs with statistical analysis
- Regression detection
- Workflow artifact generation
- GitHub-compatible markdown output
- Module benchmark workflow

## Running Tests

```bash
# Run all integration tests
python -m pytest tests/integration/ -v

# Run specific test file
python -m pytest tests/integration/test_pr_workflow.py -v

# Run excluding slow tests
python -m pytest tests/integration/ -v -m "not slow"

# Run with coverage
python -m pytest tests/integration/ --cov=. --cov-report=html
```

## Test Architecture

### Mock Benchmark Binary

The tests use a mock `valkey-benchmark` script that:
- Accepts the same CLI arguments as real valkey-benchmark
- Outputs valid CSV format
- Produces reproducible results with `--seed`
- Simulates pipeline effects on RPS

This allows testing the full workflow without:
- Building Valkey from source
- Starting a real server
- Running actual benchmarks

### Fixtures

Key fixtures in `conftest.py`:

| Fixture | Description |
|---------|-------------|
| `git_repo` | Temporary git repository |
| `mock_valkey_repo` | Git repo with mock Valkey structure |
| `mock_benchmark_binary` | Standalone mock benchmark executable |
| `minimal_benchmark_config` | Minimal valid config for fast tests |
| `minimal_config_file` | Config file on disk |
| `results_dir` | Temporary results directory |

### Helper Functions

- `create_sample_metrics()` - Generate test metrics dicts
- `write_metrics_file()` - Write metrics to JSON
- `read_metrics_file()` - Read metrics from JSON

## Adding New Tests

1. **For new workflow features**: Add to `test_pr_workflow.py`
2. **For git operations**: Add to `test_git_operations.py`
3. **For comparison logic**: Add to `test_comparison_workflow.py`
4. **For benchmark execution**: Add to `test_benchmark_execution.py`

### Example: Testing a New Comparison Feature

```python
def test_new_comparison_feature(self, tmp_path):
    """Test description."""
    # Create test metrics
    baseline_metrics = [create_sample_metrics("base", "GET", rps=100000.0)]
    new_metrics = [create_sample_metrics("new", "GET", rps=110000.0)]
    
    write_metrics_file(tmp_path / "baseline.json", baseline_metrics)
    write_metrics_file(tmp_path / "new.json", new_metrics)
    
    # Run comparison
    result = subprocess.run([
        sys.executable,
        "utils/compare_benchmark_results.py",
        "--baseline", str(tmp_path / "baseline.json"),
        "--new", str(tmp_path / "new.json"),
        "--output", str(tmp_path / "output.md"),
    ], ...)
    
    # Verify
    assert result.returncode == 0
    content = (tmp_path / "output.md").read_text()
    assert "expected content" in content
```

## CI Integration

These tests are designed to run in CI without special requirements:
- No Valkey server needed
- No database connections
- No network access
- Fast execution (~1-2 seconds total)

Add to your CI workflow:
```yaml
- name: Run integration tests
  run: python -m pytest tests/integration/ -v
```
