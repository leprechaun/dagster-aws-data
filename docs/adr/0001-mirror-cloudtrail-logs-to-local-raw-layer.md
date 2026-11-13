# Mirror CloudTrail Log Files to a local Raw layer before parsing

Log Files are copied unchanged from the AWS source bucket to MinIO (Raw), and Bronze is built only from that copy, never from AWS directly. Raw is a disposable local-network cache: AWS remains the source of truth for Log Files, so Raw can be deleted and re-pulled, and Bronze (Delta Lake on MinIO) is the data we keep. This differs from our other Dagster projects, where Raw is immutable and undeletable. The point is that changing Bronze parsing logic or rebuilding it never has to re-download history from AWS.
