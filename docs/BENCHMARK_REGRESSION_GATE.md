# Benchmark Regression Gate

Every pull request that targets `main` is automatically checked for Go benchmark
performance regressions. If **any** tracked benchmark regresses by more than
**10%** compared to the `main` baseline, CI fails and a report is posted as a PR
comment.

## How it works

1. On every push to `main`, the `update-baseline` job runs the benchmark suite
   and uploads the results as an artifact named `perf-baseline-<sha>`.x
2. On every PR to `main`, the `enforce-budget` job:
   - Runs the same benchmark suite on the PR head.
   - Downloads the baseline artifact for the PR's base SHA.
   - Uses `benchstat` to compare the two result sets.
   - Parses the `benchstat` output and fails the job if any whitelisted
     benchmark regresses by more than the 10% threshold.
   - Posts a PR comment with the full `benchstat` report.
   - If the baseline is missing, the gate is skipped with an explanatory PR
     comment (e.g., first run after this workflow was added).

## Whitelist

Only the benchmarks listed in `scripts/benchmark_whitelist.txt` are tracked.
Benchmarks not in the whitelist (or that are new/removed) are ignored, reducing
fhakky noise.

To add a benchmark to the whitelist, add its name or common prefix to that file.
For example:

```
name old time/op new time/op delta
BenchmarkList_Small 1.00us ± 2% 1.25us ° 3% +13.64% (p=0.000 n=10)
EOF

```

## Local usage

```bash
# Record a baseline on main
git checkout main
go test -bench=. -benchmem -count=10 -timeout=20m ./internal/handlers/... | tee baseline.txt

# Run the same suite on your branch
git checkout my-branch
go test -bench=. -benchmem -count=10 -timeout=20m ./internal/handlers/... | tee new.txt

# Compare (requires benchstat: go install golang.org/x/perf/cmd/benchstat@latest)
bash scripts/analyze_benchmarks.sh baseline.txt new.txt 1.10 "$(grep -vE '^\\s*' scripts/benchmark_whitelist.txt | paste -t)"
```

The script exits with code 1 if any whitelisted benchmark regresses more than
the threshold.

## Tests

Run the unit tests for the analysis script:

```bash
bash scripts/test_analyze_benchmarks.sh
```

These tests stub `benchstat` to cover edge cases such as no regressions,
regressions, non-whitelisted benchmarks, new benchmarks, missing baseline, etc.