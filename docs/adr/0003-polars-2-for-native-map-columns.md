# Use Polars 2 for native map columns, overriding dagster-delta's Polars pin

CUR 2.0 delivers some columns (e.g. `resource_tags`, `product`) as Parquet `MAP<string,string>`. Polars 1.x has no map type, so Bronze stored them as lists of key/value structs: querying a tag meant searching a list, and the Bronze type wasn't the one AWS delivers. Polars 2 has `pl.Map`, and spike #45 showed maps survive `read_parquet`/`scan_parquet` → DataFrame → dagster-delta → Delta as a Delta `map<string,string>`, including column comments, `diagonal_relaxed` stacking, and schema merges across Billing Periods.

`dagster-delta` 0.5.9 pins `polars<2.0`, so we carry a uv `override-dependencies = ["polars>=2.1.0"]` until upstream relaxes it. Polars 2 needs Python ≥3.11, so `requires-python` moves to `>=3.11`. The override is what's fragile here: a `dagster-delta` release that relies on 1.x-only Polars APIs would break us without a resolver error, so the test suite is the guard. Drop the override once `dagster-delta` allows Polars 2.

Considered: staying on Polars 1.x and keeping map columns in Arrow, writing them with `deltalake` directly. That bypasses the IO manager for one asset and splits the code path. Keeping key/value lists was the cheap option but leaves Bronze lying about AWS's types.
