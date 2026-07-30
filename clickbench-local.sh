#!/usr/bin/env bash
###############################################################################
# ClickBench on ClickHouse — local single-node run
#
# Goal of this file: teach the process. Every block below is meant to be
# readable and copy-paste-able on its own. Later, when you are comfortable,
# just run the whole thing:
#
#     ./clickbench-local.sh all
#
# Or run one step at a time (recommended the first time):
#
#     ./clickbench-local.sh preflight
#     ./clickbench-local.sh deps
#     ./clickbench-local.sh fetch
#     ./clickbench-local.sh install-clickhouse
#     ./clickbench-local.sh start
#     ./clickbench-local.sh download
#     ./clickbench-local.sh load
#     ./clickbench-local.sh bench          # <- the official, measured run
#     ./clickbench-local.sh results
#     ./clickbench-local.sh analyze
#     ./clickbench-local.sh stop
#
#     ./clickbench-local.sh help           # list all steps
#
# ---------------------------------------------------------------------------
# HOW CLICKBENCH IS STRUCTURED (as of 2026)
#
# The repo used to have one big per-system benchmark.sh. It was refactored:
# now each system directory (here: clickhouse/) contains small scripts
#
#     install     install the DBMS
#     start       start the daemon
#     check       readiness probe: `clickhouse-client --query "SELECT 1"`
#     stop        stop the daemon
#     load        create.sql + INSERT the dataset
#     query       read one SQL query on stdin, print result on stdout and the
#                 runtime in fractional seconds as the last stderr line
#     data-size   print the on-disk size of the table in bytes
#     create.sql  CREATE OR REPLACE TABLE hits (...) ENGINE = MergeTree
#     queries.sql the 43 benchmark queries, one per line
#     template.json  system metadata (name, tags, proprietary, tuned, ...)
#
# and benchmark.sh is now just:
#
#     export BENCH_DOWNLOAD_SCRIPT="download-hits-parquet-partitioned"
#     exec ../lib/benchmark-common.sh
#
# lib/benchmark-common.sh is the shared driver. Its main loop is:
#
#     ./install -> ./start -> download dataset -> ./load (timed)
#     for each of the 43 queries:
#         ./stop ; wait until ./check fails ; sync + drop_caches ;
#         ./start ; wait until ./check passes           <- TRUE COLD RUN
#         run the query 3x via ./query, print "[t1, t2, t3],"
#     ./data-size
#     concurrent QPS test (10 workers, 600 s by default)
#     ./stop
#
# t1 is the "cold" number (server restarted + page cache dropped), t2/t3 are
# the "hot" numbers. That restart-per-query is why a full run is long.
#
# ---------------------------------------------------------------------------
# WHAT "PUBLISHABLE" MEANS
#
# Official results come from run-benchmark.sh, which boots a *fresh* Ubuntu
# 24.04 EC2 VM with a 500 GB gp2 root volume, feeds it cloud-init.sh, runs
# benchmark.sh unattended, ships the log to play.clickhouse.com, and a bot
# turns that log into clickhouse/results/<YYYYMMDD>/<machine>.json.
#
# This script reproduces the same measurement by hand. On a dedicated EC2
# instance the numbers are directly comparable: the `machine` field is picked
# up from the instance metadata service, so the result JSON carries a real
# instance type. Off EC2 it falls back to a hostname label, which is fine for
# study but is not something the dashboard can place.
#
# It must be run as an ordinary user with sudo rights (the `ubuntu` user on a
# stock EC2 image); the individual privileged commands call sudo themselves.
###############################################################################

set -euo pipefail

# --------------------------------------------------------------------------- 
# Configuration. Everything is overridable from the environment, e.g.
#     BENCH_CONCURRENT_DURATION=0 BENCH_TRIES=3 ./clickbench-local.sh bench
# ---------------------------------------------------------------------------

# Where everything lives: everything is kept inside this script's own
# directory, so a plain `git clone && ./clickbench-local.sh all` in $HOME works.
#
# The one requirement is that the 'clickhouse' system user can *traverse* down
# to the parquet files: clickhouse/load symlinks them into
# /var/lib/clickhouse/user_files/ and the server follows those symlinks as that
# unprivileged user. A 0700/0750 directory on the way (Ubuntu creates home
# directories 0750) makes the INSERT fail with
#   filesystem error: in posix_stat: ... Permission denied ["...hits_70.parquet"]
# so preflight adds the missing o+x bits — see ensure_traversable below.
BASE_DIR="${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
WORK_DIR="${WORK_DIR:-$BASE_DIR}"                  # repo checkout + dataset
CB_DIR="$WORK_DIR/ClickBench"                      # the git checkout
SYSTEM="${SYSTEM:-clickhouse}"                     # which ClickBench system dir
SYS_DIR="$CB_DIR/$SYSTEM"

CB_REPO="${CB_REPO:-https://github.com/ClickHouse/ClickBench.git}"
CB_BRANCH="${CB_BRANCH:-main}"

# Label for the result JSON — upstream this is the EC2 instance type, so take
# it from the instance metadata service when we are on EC2.
ec2_instance_type() {
    local token
    token=$(curl -fsS -m 1 -X PUT http://169.254.169.254/latest/api/token \
            -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null) || return 1
    curl -fsS -m 1 -H "X-aws-ec2-metadata-token: $token" \
        http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null
}
MACHINE="${MACHINE:-$(ec2_instance_type || echo "local-$(hostname)")}"

# Driver knobs (read by lib/benchmark-common.sh). Defaults shown are upstream's.
export BENCH_TRIES="${BENCH_TRIES:-3}"                                 # runs per query
export BENCH_CONCURRENT_CONNECTIONS="${BENCH_CONCURRENT_CONNECTIONS:-10}"
export BENCH_CONCURRENT_DURATION="${BENCH_CONCURRENT_DURATION:-600}"   # 0 = skip QPS test
export HOME="${HOME:-$(getent passwd "$(id -u)" | cut -d: -f6)}"        # driver expects a real HOME

LOG="$SYS_DIR/log"                                 # full benchmark log

# The reference machine every ClickBench number is normalised against.
REF_MACHINE="c6a.4xlarge (16 vCPU, 32 GiB RAM, 500 GB gp2)"

say()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[warn] %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m[fail] %s\033[0m\n' "$*" >&2; exit 1; }

# The server reads the dataset as the 'clickhouse' user, so every directory on
# the way to the parquet files must be traversable by it. Add the missing o+x
# bits rather than moving the data somewhere else; o+x on a directory grants
# traversal only, not the right to list it.
ensure_traversable() {
    local dir="$1"
    while [ "$dir" != "/" ]; do
        if [ ! "$(stat -c %A "$dir" | cut -c10)" = "x" ]; then
            say "Making $dir traversable for the clickhouse user (chmod o+x)"
            sudo chmod o+x "$dir"
        fi
        dir=$(dirname "$dir")
    done
}

assert_daemon_can_read() {
    local path="$1"
    id clickhouse >/dev/null 2>&1 || return 0     # not installed yet
    sudo -u clickhouse test -r "$path" && return 0
    ensure_traversable "$(dirname "$path")"
    sudo -u clickhouse test -r "$path" && return 0
    warn "User 'clickhouse' cannot read $path"
    die  "Check the mode of every parent directory: ls -ld \$(namei -l $path)"
}

# Upstream clickhouse/load runs
#     sudo chown -h clickhouse:clickhouse /var/lib/clickhouse/user_files/hits_*.parquet
#     sudo rm -f                          /var/lib/clickhouse/user_files/hits_*.parquet
# where the glob is expanded by the calling shell, not by sudo. Those
# directories are 0700/0750 clickhouse:clickhouse, so for a non-root caller the
# pattern stays literal: chown fails and `set -e` aborts the load. Upstream runs
# as root via cloud-init and never sees it. Grant the least that makes the
# unmodified upstream script work — traverse on the data dir, list on
# user_files; the per-table directories below stay unreadable.
allow_user_files_glob() {
    [ -d /var/lib/clickhouse/user_files ] || return 0
    sudo chmod o+x  /var/lib/clickhouse
    sudo chmod o+rx /var/lib/clickhouse/user_files
}

###############################################################################
# STEP: preflight — is this box able to run the benchmark honestly?
###############################################################################
step_preflight() {
    say "Preflight"

    # Everything privileged goes through sudo; running the whole script as root
    # is not needed and leaves root-owned files in the work directory.
    if [ "$(id -u)" -eq 0 ]; then
        warn "Running as root. This script is meant to be run as an ordinary user with sudo rights."
    fi
    sudo true || die "sudo is required — the privileged steps call it directly."

    # ClickBench standardises on Ubuntu 24.04+. Older is usually fine for
    # ClickHouse itself (the installer ships a static binary), but note it.
    . /etc/os-release
    echo "OS:        $PRETTY_NAME"
    if [[ "$VERSION_ID" < "24.04" ]]; then
        warn "ClickBench standardises on Ubuntu 24.04+; you are on $VERSION_ID."
    fi

    echo "CPU:       $(nproc) logical cores — $(lscpu | sed -n 's/^Model name: *//p' | head -1)"
    echo "RAM:       $(free -g | awk '/^Mem:/ {print $2}') GiB"
    echo "Machine:   $MACHINE"
    echo "Reference: $REF_MACHINE"

    # Space: ~14 GB of parquet + ~15 GB of MergeTree data + room for merges.
    local avail_work avail_var
    mkdir -p "$WORK_DIR"
    avail_work=$(df -BG --output=avail "$WORK_DIR" | tail -1 | tr -dc '0-9')
    avail_var=$(df -BG --output=avail /var 2>/dev/null | tail -1 | tr -dc '0-9')
    echo
    echo "Free space in $WORK_DIR: ${avail_work} GiB (need ~20 for the parquet dataset)"
    echo "Free space in /var:      ${avail_var} GiB (need ~30 for /var/lib/clickhouse)"
    (( avail_work >= 25 )) || die "Not enough space for the dataset."
    (( avail_var  >= 35 )) || die "Not enough space for /var/lib/clickhouse."

    # A non-traversable directory anywhere above the dataset breaks the load
    # step; Ubuntu creates home directories 0750, so this usually fixes one.
    say "Checking that the clickhouse user can reach $WORK_DIR"
    ensure_traversable "$WORK_DIR"
    assert_daemon_can_read "$WORK_DIR" && echo "OK — path is traversable."

    # The driver drops the page cache before every cold run. Without this the
    # cold numbers are fiction.
    say "Checking that we can drop the OS page cache (needed for cold runs)"
    sync && echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null \
        || die "Cannot write /proc/sys/vm/drop_caches — cold runs would be fake."
    echo "OK — page cache can be dropped."

    # Nothing else should be competing for the machine during a run.
    say "Current load (should be ~idle before a real run)"
    uptime
}

###############################################################################
# STEP: deps — packages the driver and this script rely on
###############################################################################
step_deps() {
    say "Installing prerequisites"
    # wget: dataset download (100 parallel transfers)
    # jq:   reads template.json, and we use it to build the result JSON
    # git:  the ClickBench checkout
    # python3: used by the driver to build the per-connection query permutation
    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get update -y
    sudo apt-get install -y wget curl git jq python3
}

###############################################################################
# STEP: fetch — clone ClickBench
###############################################################################
step_fetch() {
    say "Fetching ClickBench into $CB_DIR"
    mkdir -p "$WORK_DIR"
    if [ -d "$CB_DIR/.git" ]; then
        git -C "$CB_DIR" fetch --depth 1 origin "$CB_BRANCH"
        git -C "$CB_DIR" reset --hard "origin/$CB_BRANCH"
    else
        git clone --depth 1 "$CB_REPO" --branch "$CB_BRANCH" "$CB_DIR"
    fi

    say "The clickhouse system directory — read these, they are all tiny"
    ls -la "$SYS_DIR"
    echo
    echo "--- benchmark.sh ---";           cat "$SYS_DIR/benchmark.sh"
    echo "--- install ---";                cat "$SYS_DIR/install"
    echo "--- load ---";                   cat "$SYS_DIR/load"
    echo "--- query ---";                  cat "$SYS_DIR/query"
    echo "--- data-size ---";              cat "$SYS_DIR/data-size"
    echo "--- template.json ---";          cat "$SYS_DIR/template.json"
    echo
    echo "43 queries in queries.sql: $(wc -l < "$SYS_DIR/queries.sql")"
}

###############################################################################
# STEP: install-clickhouse
#
# This is verbatim what clickhouse/install does. Two things happen:
#   1. the official one-liner installer drops a static `clickhouse` binary and
#      registers the server/client symlinks + systemd-less init;
#   2. a config.d snippet forces *eager* startup loading, so the first query
#      after a restart does not silently pay for lazy part/primary-key loading.
#      That snippet is part of the official ClickBench setup — it is not
#      "tuning", it just moves work out of the measured query window.
###############################################################################
step_install_clickhouse() {
    say "Installing ClickHouse"
    cd "$SYS_DIR"
    ./install
    allow_user_files_glob
    echo
    clickhouse-client --version || true
    echo
    echo "--- /etc/clickhouse-server/config.d/eager_load.yaml ---"
    sudo cat /etc/clickhouse-server/config.d/eager_load.yaml
}

###############################################################################
# STEP: start / stop / check — the daemon lifecycle the driver uses
###############################################################################
step_start() {
    say "Starting the server and probing it"
    cd "$SYS_DIR"
    # `clickhouse start` exits 2 when the server is already up. lib/benchmark-common.sh
    # ignores ./start's status for the same reason and treats ./check as the
    # authoritative readiness signal; do the same here.
    ./start || true
    # ./check is just `clickhouse-client --query "SELECT 1"`. The driver polls
    # it for up to 300 s after every restart.
    for _ in $(seq 1 60); do ./check >/dev/null 2>&1 && break; sleep 1; done
    ./check || die "Server did not become ready within 60 s."
    echo "Server is up."
    clickhouse-client --query "SELECT version(), uptime()"
}

step_stop() {
    say "Stopping the server"
    cd "$SYS_DIR"
    ./stop
}

# The driver stops the server when a run ends, so anything that queries the
# server afterwards has to bring it back up first.
ensure_server_up() {
    cd "$SYS_DIR"
    ./check >/dev/null 2>&1 && return 0
    say "Server is down (the driver stops it at the end of a run) — starting it"
    ./start || true
    for _ in $(seq 1 60); do ./check >/dev/null 2>&1 && return 0; sleep 1; done
    return 1
}

###############################################################################
# STEP: download — the dataset
#
# ClickHouse uses the 100-file partitioned parquet variant
# (hits_0.parquet .. hits_99.parquet, ~14 GB total, 99,997,497 rows).
# The download script fires 100 parallel wgets with --continue, so re-running
# it is cheap and resumable.
#
# Other formats exist if you ever want to compare loaders:
#   https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz
#   https://datasets.clickhouse.com/hits_compatible/hits.csv.gz
#   https://datasets.clickhouse.com/hits_compatible/hits.parquet   (single file)
###############################################################################
step_download() {
    say "Downloading the hits dataset (100 parquet files) into $SYS_DIR"
    cd "$SYS_DIR"
    ../lib/download-hits-parquet-partitioned
    echo
    du -sh "$SYS_DIR"/hits_*.parquet | tail -1
    echo "Files: $(ls -1 "$SYS_DIR"/hits_*.parquet | wc -l) (expect 100)"
    assert_daemon_can_read "$SYS_DIR/hits_0.parquet"
}

###############################################################################
# STEP: load — create the table and ingest
#
# clickhouse/load does:
#   clickhouse-client < create.sql            # CREATE OR REPLACE TABLE hits
#   symlink the parquets into /var/lib/clickhouse/user_files/
#   INSERT INTO hits SELECT * FROM file('hits_*.parquet')
#       --max-insert-threads $(( nproc / 4 ))
#
# Note create.sql sets fsync_after_insert = 1, so "Load time" includes making
# the data durable. Running this step is idempotent (CREATE OR REPLACE).
#
# IMPORTANT: the load time that ends up in the result JSON is the one measured
# by the driver during `bench`, not this one. Doing it here is just to see it.
###############################################################################
step_load() {
    say "Loading the dataset (this is the same ./load the driver times)"
    cd "$SYS_DIR"
    allow_user_files_glob
    assert_daemon_can_read "$SYS_DIR/hits_0.parquet"
    local t0 t1
    t0=$(date +%s.%N)
    ./load
    sync
    t1=$(date +%s.%N)
    awk -v s="$t0" -v e="$t1" 'BEGIN { printf "Load time: %.3f s\n", e - s }'

    say "Sanity checks"
    clickhouse-client --query "SELECT count() FROM hits"          # expect 99997497
    clickhouse-client --query "SELECT formatReadableSize(total_bytes) FROM system.tables WHERE database='default' AND name='hits'"
    ./data-size
}

###############################################################################
# STEP: bench — THE OFFICIAL RUN
#
# This re-runs everything from scratch through the shared driver: install (no-op
# if already installed), start, download (resumes / no-op), load (timed), then
# the 43-query sweep with a stop/drop_caches/start cycle before every query,
# then data-size, then the concurrent-QPS window.
#
# The preamble lines (System name / Proprietary / Tuned / Tags) mirror what
# cloud-init.sh.in writes upstream, so the log has the same shape as a real one.
#
# Knobs for a fast smoke test (NOT publishable):
#     BENCH_CONCURRENT_DURATION=0 BENCH_TRIES=1 ./clickbench-local.sh bench
###############################################################################
step_bench() {
    say "Running the full benchmark — output is tee'd to $LOG"
    cd "$SYS_DIR"
    allow_user_files_glob

    : > "$LOG"
    {
        echo -n 'System name: ';  jq -r    '.system'      template.json
        echo -n 'Proprietary: ';  jq -r    '.proprietary' template.json
        echo -n 'Tuned: ';        jq -r    '.tuned'       template.json
        echo -n 'Tags: ';         jq -c -r '.tags'        template.json
        echo "Machine: $MACHINE"
        echo -n 'Disk usage before: '; df -B1 / | tail -n1 | awk '{ print $3 }'
    } | tee -a "$LOG"

    local t0=$SECONDS
    ./benchmark.sh 2>&1 | tee -a "$LOG"

    {
        echo -n 'Disk usage after: '; df -B1 / | tail -n1 | awk '{ print $3 }'
        echo "Total time: $((SECONDS - t0))"
    } | tee -a "$LOG"

    say "Interesting lines from the log"
    grep -E '^(Load time|Data size|Concurrent )' "$LOG" || true
    echo "Query result lines: $(grep -cE '^\[' "$LOG") (expect 43)"
}

###############################################################################
# STEP: sweep — queries only, no reload
#
# For iterating on performance questions. It reuses the *real* driver functions
# (benchmark-common.sh only auto-runs bench_main when executed, not when
# sourced), so the cold-cycle semantics are identical to `bench`; it just skips
# install/download/load and the QPS window.
#
# Output: the same "[t1, t2, t3]," lines, plus result.csv (<query>,<try>,<secs>).
###############################################################################
step_sweep() {
    say "Query sweep only (assumes the table is already loaded)"
    cd "$SYS_DIR"
    clickhouse-client --query "SELECT count() FROM hits" >/dev/null \
        || die "Table 'hits' is not there — run the load step first."

    export BENCH_DOWNLOAD_SCRIPT=""      # required by the driver; empty = no download
    # shellcheck disable=SC1091
    source ../lib/benchmark-common.sh

    : > result.csv
    local n=1
    while IFS= read -r q; do
        [ -z "$q" ] && continue
        bench_run_query "$q" "$n"
        n=$((n + 1))
    done < queries.sql
    bench_stop || true
    echo "Per-try timings written to $SYS_DIR/result.csv"
}

###############################################################################
# STEP: results — turn the log into a ClickBench result JSON
#
# Upstream this is done by materialized views on play.clickhouse.com; the shape
# is documented by clickhouse/results/<date>/<machine>.json. We build the same
# object locally from template.json + the parsed log.
###############################################################################
step_results() {
    say "Building the result JSON from $LOG"
    [ -s "$LOG" ] || die "No log at $LOG — run the bench step first."
    cd "$SYS_DIR"

    local load_time data_size qps err_ratio results n date_dir out
    load_time=$(grep -m1 '^Load time: ' "$LOG" | awk '{printf "%.0f", $3}')
    data_size=$(grep -m1 '^Data size: ' "$LOG" | awk '{print $3}')
    qps=$(grep -m1 '^Concurrent QPS: ' "$LOG" | awk '{print $3}')
    err_ratio=$(grep -m1 '^Concurrent error ratio: ' "$LOG" | awk '{print $4}')
    : "${qps:=null}" "${err_ratio:=null}"

    # Each measured query prints "[t1, t2, t3]," — strip the trailing comma and
    # slurp the lines into a JSON array of arrays. "null" entries stay null.
    results=$(grep -E '^\[' "$LOG" | sed 's/,$//' | jq -s .)
    n=$(jq 'length' <<< "$results")
    [ "$n" -eq 43 ] || warn "Got $n query rows, expected 43 (partial run?)"

    date_dir=$(date -u +%Y%m%d)
    mkdir -p "results/$date_dir"
    out="results/$date_dir/$MACHINE.json"

    jq -n \
        --slurpfile t template.json \
        --arg   date "$(date -u +%Y-%m-%d)" \
        --arg   machine "$MACHINE" \
        --argjson load "$load_time" \
        --argjson size "$data_size" \
        --argjson qps "$qps" \
        --argjson err "$err_ratio" \
        --argjson res "$results" \
        '{
            system:                 $t[0].system,
            date:                   $date,
            machine:                $machine,
            cluster_size:           1,
            proprietary:            $t[0].proprietary,
            hardware:               $t[0].hardware,
            tuned:                  $t[0].tuned,
            tags:                   $t[0].tags,
            load_time:              $load,
            data_size:              $size,
            concurrent_qps:         $qps,
            concurrent_error_ratio: $err,
            result:                 $res
         }' > "$out"

    jq empty "$out" && say "Wrote $SYS_DIR/$out"
    jq 'del(.result)' "$out"

    # The repo ships a validator for result shapes; use it when present.
    if [ -f "$CB_DIR/validate-results.py" ]; then
        say "Validating"
        (cd "$CB_DIR" && python3 validate-results.py "$SYSTEM/$out") || \
            warn "Validator complained — check the report above."
    fi
}

###############################################################################
# STEP: analyze — performance characteristics of the run
#
# Two views:
#   1. result.csv / the log -> cold vs hot per query, biggest gaps.
#   2. system.query_log     -> rows/bytes read, memory, threads per query.
# Together they tell you which queries are page-cache/IO bound (huge t1 vs t2),
# which are memory-bandwidth bound, and which saturate cores.
###############################################################################
step_analyze() {
    say "Cold vs hot per query (from $LOG)"
    grep -E '^\[' "$LOG" | sed 's/[][,]/ /g' | \
    awk '{ q++; cold=$1; hot=($2<$3?$2:$3);
           printf "Q%-3d cold %8.3f s   hot %8.3f s   cold/hot %6.1fx\n",
                  q-1, cold, hot, (hot>0 ? cold/hot : 0) }' | \
    sort -k9 -gr | head -20

    say "Sum of cold and hot times"
    grep -E '^\[' "$LOG" | sed 's/[][,]/ /g' | \
    awk '{ c+=$1; h+=($2<$3?$2:$3) } END { printf "cold total %.1f s, hot total %.1f s\n", c, h }'

    ensure_server_up || { warn "Server will not start — skipping the SQL sections."; return 0; }

    # query_log is written from an in-memory buffer every 7.5 s; without this
    # the queries that just ran are missing from it.
    clickhouse-client --query "SYSTEM FLUSH LOGS"

    say "Heaviest queries by data read (from system.query_log)"
    clickhouse-client --query "
        SELECT
            substring(query, 1, 60)              AS q,
            formatReadableSize(read_bytes)       AS read,
            formatReadableQuantity(read_rows)    AS rows,
            formatReadableSize(memory_usage)     AS mem,
            round(query_duration_ms / 1000, 3)   AS sec
        FROM system.query_log
        WHERE type = 'QueryFinish' AND event_time > now() - INTERVAL 1 DAY
          AND query NOT LIKE '%system.%'
        ORDER BY read_bytes DESC
        LIMIT 15
        FORMAT PrettyCompact" || warn "query_log is empty for this window."

    say "Table storage breakdown (top 15 columns by compressed size)"
    clickhouse-client --query "
        SELECT
            column,
            formatReadableSize(sum(column_data_compressed_bytes))   AS compressed,
            formatReadableSize(sum(column_data_uncompressed_bytes)) AS uncompressed,
            round(sum(column_data_uncompressed_bytes) / sum(column_data_compressed_bytes), 2) AS ratio
        FROM system.parts_columns
        WHERE table = 'hits' AND active
        GROUP BY column ORDER BY sum(column_data_compressed_bytes) DESC
        LIMIT 15
        FORMAT PrettyCompact" || warn "Could not read system.parts_columns."
}

###############################################################################
# STEP: clean — free the disk / start over
###############################################################################
step_clean() {
    say "Removing the downloaded parquet files (keeps the loaded table)"
    rm -f "$SYS_DIR"/hits_*.parquet
    echo "To also drop the table:   clickhouse-client --query 'DROP TABLE IF EXISTS hits'"
    echo "To remove the checkout:   rm -rf $CB_DIR"
}

###############################################################################
# Dispatcher
###############################################################################
step_all() {
    step_preflight
    step_deps
    step_fetch
    step_install_clickhouse
    step_start
    step_bench          # re-does download + load itself, timed correctly
    step_results
    step_analyze
}

step_help() {
    sed -n 's/^# \{0,1\}//p' <<'EOF'
# Steps:
#   preflight           check OS, disk, RAM, drop_caches permission
#   deps                apt-get install wget curl git jq python3
#   fetch               clone ClickBench and dump the clickhouse/* scripts
#   install-clickhouse  run clickhouse/install (binary + eager-load config)
#   start | stop        daemon lifecycle
#   download            fetch the 100 parquet files (~14 GB)
#   load                create.sql + INSERT, with sanity checks
#   bench               the official measured run -> log
#   sweep               43-query cold/hot sweep only, no reload
#   results             parse the log into results/<date>/<machine>.json
#   analyze             cold-vs-hot breakdown + query_log + storage stats
#   clean               delete the parquet files
#   all                 preflight -> ... -> bench -> results -> analyze
#
# Useful overrides:
#   MACHINE=c7i.4xlarge                label in the result JSON
#                                      (defaults to the EC2 instance type)
#   WORK_DIR=/mnt/data/clickbench      where the checkout + dataset live
#                                      (defaults to this script's directory)
#   BENCH_TRIES=1                      fewer runs per query (smoke test)
#   BENCH_CONCURRENT_DURATION=0        skip the 600 s concurrency window
EOF
}

case "${1:-help}" in
    preflight)          step_preflight ;;
    deps)               step_deps ;;
    fetch)              step_fetch ;;
    install-clickhouse) step_install_clickhouse ;;
    start)              step_start ;;
    stop)               step_stop ;;
    download)           step_download ;;
    load)               step_load ;;
    bench)              step_bench ;;
    sweep)              step_sweep ;;
    results)            step_results ;;
    analyze)            step_analyze ;;
    clean)              step_clean ;;
    all)                step_all ;;
    help|-h|--help)     step_help ;;
    *)                  die "Unknown step '$1' — try: $0 help" ;;
esac
