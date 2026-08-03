# clickhouse-scripts

Scripts for running the [ClickBench](https://github.com/ClickHouse/ClickBench)
hardware and cross-database suites against ClickHouse. Both runners default to
the hardware suite used by <https://benchmark.clickhouse.com/hardware/>; pass
`--suite clickbench` for the main cross-database suite.

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
./clickbench-local.sh all           # hardware suite (default)
./clickbench-local.sh --suite clickbench all
```

Hardware mode runs the unmodified upstream `hardware/hardware.sh`, writes a
`hardware/results/*.json` contribution, and regenerates `hardware/index.html`.
Set `HARDWARE_MACHINE`, `HARDWARE_COMMENT`, and `HARDWARE_RESULT_NAME` to record
the exact instance, CPU generation, memory, and storage configuration.

## `clickbench-cluster.sh` — AWS runner

By default this provisions one EC2 instance and runs the hardware suite. For an
EBS-backed C8i contribution:

```bash
MACHINE=c8i.4xlarge \
HARDWARE_MACHINE="AWS c8i.4xlarge (Intel Xeon 6)" \
HARDWARE_COMMENT="AWS c8i.4xlarge, 16 vCPU, 32 GiB RAM, 500 GB gp3 EBS" \
./clickbench-cluster.sh provision
./clickbench-cluster.sh bootstrap
./clickbench-cluster.sh run
./clickbench-cluster.sh status
./clickbench-cluster.sh fetch-log
./clickbench-cluster.sh results
./clickbench-cluster.sh submit
./clickbench-cluster.sh terminate
```

For `i7i.2xlarge`, set `HARDWARE_STORAGE=instance-store`; bootstrap formats and
mounts its first local NVMe device as the benchmark working directory. The mode
intentionally uses one device, so multi-device shapes need an explicit RAID or
single-device test plan. This is designed for fresh benchmark instances and
must not be used on a device holding data you need to retain.

With `--suite clickbench`, the AWS runner preserves the original three-node
workflow: three shards, no replicas or Keeper, and unmodified queries against a
`Distributed` table. Use the same suite flag on every command because each mode
has separate node and log state files.

## Notes

- Run both scripts as an ordinary user with sudo rights (the `ubuntu` user on a
  stock EC2 image); the privileged commands call `sudo` themselves.
- The ClickBench checkout and dataset live in this directory
  (override with `WORK_DIR=`). `clickhouse-server` reads the parquet files as
  the unprivileged `clickhouse` user, so `preflight` adds the missing `o+x`
  bits to the parent directories — Ubuntu creates home directories `0750`.
- `nodes.env` holds node addresses and is generated per deployment — it is
  gitignored and must never be committed.
