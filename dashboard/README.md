# Dashboard

This folder contains a lightweight React-based dashboard used to visualize benchmark results.

The static files are uploaded to an Amazon S3 bucket via the `dashboard_sync.yml` workflow.
Benchmark metrics (`completed_commits.json` and the `results/` directory) are stored in the same bucket so the dashboard can fetch them directly. Each entry in
`completed_commits.json` includes the commit SHA, its original timestamp, and the benchmarking status. Entries with `status: in_progress` are ignored by the dashboard.

