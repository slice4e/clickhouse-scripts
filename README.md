# clickhouse-scripts

Scripts for running [ClickBench](https://github.com/ClickHouse/ClickBench) against
ClickHouse, aimed at producing results that are comparable to — and eventually
submittable alongside — the ones on <https://benchmark.clickhouse.com/>.

Both scripts are written to be read: every step is a separate command with the
reasoning in comments, so you can copy-paste your way through the process the
first time and just run the whole thing afterwards.

## `clickbench-local.sh` — single node

Installs ClickHouse, fetches the 100-file partitioned `hits` dataset (~14 GB,
99,997,497 rows), loads it, and runs the official 43-query sweep through the
upstream driver.

```bash
./clickbench-local.sh help
./clickbench-local.sh preflight     # OS, disk, RAM, drop_caches permission
./clickbench-local.sh all           # or one step at a time
```

Run it on a fresh EC2 instance and it produces a result JSON of the same shape
the ClickBench fleet publishes. Use `MACHINE=c7i.4xlarge` so the `machine` field
is the real instance type.

## `clickbench-cluster.sh` — 3-node cluster on AWS

Provisions three EC2 instances, generates a complete `clickhouse-cluster`
ClickBench system directory (3 shards, no replicas, no Keeper, unmodified
queries against a `Distributed` table), runs the benchmark across them, and
builds a result with `cluster_size: 3`.

```bash
./clickbench-cluster.sh help
```

## Notes

- The work directory defaults to `/var/lib/clickbench`, deliberately not under
  `$HOME`: `clickhouse-server` reads the parquet files as the unprivileged
  `clickhouse` user and cannot traverse a `0700` home directory.
- `nodes.env` holds node addresses and is generated per deployment — it is
  gitignored and must never be committed.
