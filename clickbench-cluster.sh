#!/usr/bin/env bash
###############################################################################
# ClickBench on a 3-node ClickHouse cluster on AWS — publishable run
#
# Companion to clickbench-local.sh. Read that one first: everything about the
# 43 queries, the cold/hot cycle, the driver and the result JSON is the same.
# This file only deals with what *changes* when there is more than one node.
#
# ---------------------------------------------------------------------------
# WHAT UPSTREAM DOES AND DOES NOT GIVE US
#
# ClickBench has no multi-node self-managed entry today. Checked in the repo:
#   - `cluster_size` is a first-class field in every result JSON, and the
#     dashboard renders anything >1 as "ClickHouse (3×c6a.4xlarge)";
#   - but the only entries using it are managed services (clickhouse-cloud,
#     Databricks, Snowflake, ByteHouse, CHYT). Nothing self-hosted;
#   - run-benchmark.sh launches exactly ONE EC2 instance and hands it to
#     cloud-init, which shows the log to play.clickhouse.com where a bot turns
#     it into the results JSON. There is no 3-node path in that automation.
#
# Consequence: the run is operator-driven (this script), and we build the
# result JSON ourselves. That is explicitly allowed — README says "put the
# .json files with the results for every hardware configuration into this
# directory". generate-results.sh discovers any new <system>/results/ dir
# automatically, so nothing else needs patching.
#
# ---------------------------------------------------------------------------
# TOPOLOGY
#
#   node0 (initiator)      node1                  node2
#   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
#   │ clickhouse-server│   │ clickhouse-server│   │ clickhouse-server│
#   │ hits_local (1/3) │   │ hits_local (1/3) │   │ hits_local (1/3) │
#   │ hits = Distributed────────────┴──────────────────────┘         │
#   │ clickhouse-client│   └──────────────────┘   └──────────────────┘
#   └──────────────────┘
#
#   3 shards, 0 replicas. No ClickHouse Keeper: Keeper is only needed for
#   ReplicatedMergeTree and ON CLUSTER DDL, and we have neither — the DDL is
#   sent to each shard individually over the native protocol.
#
#   The 43 queries are unchanged: all of them are plain `... FROM hits` with
#   no joins and no subqueries, so a Distributed table named `hits` makes
#   queries.sql work verbatim. That is what keeps this comparable to the
#   single-node entry.
#
# ---------------------------------------------------------------------------
# THE FOUR THINGS THAT ARE GENUINELY HARDER THAN SINGLE-NODE
#
#   1. True cold runs. The rules require restarting the database and dropping
#      the page cache before the first run of every query. On a cluster that
#      means ALL THREE nodes, not just the one you are typing on. The driver
#      only drops the local cache — but it calls an optional ./flush-caches
#      hook right after, which is exactly where the fan-out belongs.
#   2. Loading. Each shard ingests the parquet files it holds, in parallel.
#      A cluster loading through one node would measure the network, not the
#      database. Documented in the generated README so nobody has to guess.
#   3. Data size. Sum of the shards, via clusterAllReplicas(...).
#   4. Readiness. ./check must verify all three nodes, otherwise the driver
#      happily benchmarks a two-node cluster after a node fails to come back.
#
# ---------------------------------------------------------------------------
# HOW TO RUN IT — three phases
#
#   Phase 1, here (needs awscli + credentials):
#       ./clickbench-cluster.sh preflight
#       ./clickbench-cluster.sh provision     # 3 × c6a.4xlarge, ~2 $/hour
#       ./clickbench-cluster.sh gen           # write the system dir locally
#       ./clickbench-cluster.sh bootstrap     # ssh trust + push the dir
#
#   Phase 2, the measured run (started here, executes on node0):
#       ./clickbench-cluster.sh run           # detached; survives a hangup
#       ./clickbench-cluster.sh status        # tail the log, repeat as needed
#
#   Phase 3, here:
#       ./clickbench-cluster.sh fetch-log
#       ./clickbench-cluster.sh results       # -> clickhouse-cluster/results/
#       ./clickbench-cluster.sh submit        # validate + PR checklist
#       ./clickbench-cluster.sh terminate     # STOP PAYING (asks first)
#
#   ./clickbench-cluster.sh help
###############################################################################

set -euo pipefail

BASE_DIR="${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"

# The name of the new ClickBench system directory. This is the PR artifact.
SYSTEM="${SYSTEM:-clickhouse-cluster}"
GEN_DIR="$BASE_DIR/$SYSTEM"

# Cluster shape.
NODE_COUNT="${NODE_COUNT:-3}"
CLUSTER_NAME="${CLUSTER_NAME:-cb}"

# EC2. c6a.4xlarge + 500 GB gp2 + Ubuntu 24.04 is the ClickBench reference
# configuration; keep it so the 3-node numbers sit next to the 1-node ones.
MACHINE="${MACHINE:-c6a.4xlarge}"
VOLUME_SIZE="${VOLUME_SIZE:-500}"
VOLUME_TYPE="${VOLUME_TYPE:-gp2}"
KEY_NAME="${KEY_NAME:-clickbench}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/$KEY_NAME.pem}"
SSH_USER="${SSH_USER:-ubuntu}"
SG_NAME="${SG_NAME:-clickbench-cluster}"
PG_NAME="${PG_NAME:-clickbench-cluster}"
TAG_NAME="${TAG_NAME:-clickbench-cluster}"

# Where the node-side checkout lives. Same reasoning as the single-node
# script: NOT under a 0700 home directory, because clickhouse-server reads the
# parquet files as the unprivileged 'clickhouse' user.
NODE_WORK_DIR="${NODE_WORK_DIR:-/var/lib/clickbench}"

NODES_FILE="$BASE_DIR/nodes.env"
LOG="$BASE_DIR/cluster-log"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"

say()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[warn] %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m[fail] %s\033[0m\n' "$*" >&2; exit 1; }

load_nodes() {
    [ -f "$NODES_FILE" ] || die "No $NODES_FILE — run the provision step first."
    # shellcheck disable=SC1090
    . "$NODES_FILE"
}

# ssh to a node from here, by public IP.
ssh_pub() { local host="$1"; shift; ssh $SSH_OPTS -i "$SSH_KEY" "$SSH_USER@$host" "$@"; }

###############################################################################
# STEP: preflight — can this workstation drive an AWS run at all?
###############################################################################
step_preflight() {
    say "Preflight"
    command -v aws >/dev/null || die "awscli is not installed (apt-get install -y awscli, or the v2 bundle)."
    aws sts get-caller-identity --output text >/dev/null 2>&1 \
        || die "AWS credentials are not usable — configure them first."
    echo "AWS identity: $(aws sts get-caller-identity --query Arn --output text)"
    echo "Region:       $(aws configure get region || echo '<unset — export AWS_DEFAULT_REGION>')"
    command -v jq >/dev/null || die "jq is required."

    if [ ! -f "$SSH_KEY" ]; then
        warn "No SSH key at $SSH_KEY."
        echo "Create a dedicated throwaway key pair for the benchmark:"
        echo "    aws ec2 create-key-pair --key-name $KEY_NAME \\"
        echo "        --query KeyMaterial --output text > $SSH_KEY && chmod 600 $SSH_KEY"
        echo "Dedicated because a copy of the private key has to live on node0"
        echo "(the run fans out over ssh for hours, so agent forwarding cannot"
        echo "be used — it dies with your shell)."
    else
        echo "SSH key:      $SSH_KEY"
        [ "$(stat -c %a "$SSH_KEY")" = "600" ] || warn "chmod 600 $SSH_KEY"
    fi

    cat <<EOF

Cost, roughly, on-demand in us-east-1:
    3 × $MACHINE            ~\$1.85 / hour
    3 × ${VOLUME_SIZE} GB $VOLUME_TYPE           ~\$0.21 / hour
A full run (install + 14 GB × 3 download + load + 43 queries × 3 tries with a
cluster restart before each + the 600 s concurrency window) takes on the order
of two hours. Budget a few dollars — and do not forget the terminate step.
EOF
}

###############################################################################
# STEP: provision — security group, placement group, 3 instances
#
# Everything here is idempotent: re-running finds the existing SG/PG. Only
# run-instances actually creates something new, so do not run it twice by
# accident (check nodes.env first).
###############################################################################
step_provision() {
    say "Resolving the network"
    local subnet az vpc ami sg pg ids

    # Default VPC, one subnet — every node must be in the SAME AZ, otherwise
    # inter-node traffic is cross-AZ (billed, and slower).
    subnet=$(aws ec2 describe-subnets --filters Name=default-for-az,Values=true \
             --query 'Subnets[0].SubnetId' --output text)
    [ "$subnet" != "None" ] || die "No default subnet found — pass SUBNET_ID explicitly."
    subnet="${SUBNET_ID:-$subnet}"
    az=$(aws ec2 describe-subnets --subnet-ids "$subnet" --query 'Subnets[0].AvailabilityZone' --output text)
    vpc=$(aws ec2 describe-subnets --subnet-ids "$subnet" --query 'Subnets[0].VpcId' --output text)
    echo "subnet=$subnet az=$az vpc=$vpc"

    # Same AMI query run-benchmark.sh uses, so we boot the same image the
    # official single-node runs use.
    ami=$(aws ec2 describe-images --owners amazon \
          --filters "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04*" \
                    "Name=architecture,Values=x86_64" "Name=state,Values=available" \
          --query 'sort_by(Images, &CreationDate) | [-1].ImageId' --output text)
    echo "ami=$ami (Ubuntu 24.04)"

    say "Security group"
    # The cluster talks on 9000 (native) and 8123 (http) with NO password on
    # the default user — the security group is the only thing protecting it.
    # So: all traffic between members of the group, and ssh from this host's
    # address only. Never open 9000 to 0.0.0.0/0.
    sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$SG_NAME" "Name=vpc-id,Values=$vpc" \
         --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo None)
    if [ "$sg" = "None" ] || [ -z "$sg" ]; then
        sg=$(aws ec2 create-security-group --group-name "$SG_NAME" --vpc-id "$vpc" \
             --description "ClickBench 3-node ClickHouse cluster" --query GroupId --output text)
        aws ec2 authorize-security-group-ingress --group-id "$sg" --protocol -1 --source-group "$sg" >/dev/null
        local myip; myip=$(curl -fsS https://checkip.amazonaws.com | tr -d '\n')
        aws ec2 authorize-security-group-ingress --group-id "$sg" --protocol tcp --port 22 --cidr "$myip/32" >/dev/null
        echo "created $sg (intra-group any/any, ssh from $myip/32)"
    else
        echo "reusing $sg"
    fi

    say "Placement group (cluster strategy — lowest inter-node latency)"
    aws ec2 create-placement-group --group-name "$PG_NAME" --strategy cluster >/dev/null 2>&1 \
        && echo "created $PG_NAME" || echo "reusing $PG_NAME"
    pg="$PG_NAME"

    say "Launching $NODE_COUNT × $MACHINE"
    ids=$(aws ec2 run-instances --image-id "$ami" --instance-type "$MACHINE" \
        --count "$NODE_COUNT" --key-name "$KEY_NAME" \
        --security-group-ids "$sg" --subnet-id "$subnet" \
        --placement "GroupName=$pg,AvailabilityZone=$az" \
        --block-device-mappings "DeviceName=/dev/sda1,Ebs={DeleteOnTermination=true,VolumeSize=$VOLUME_SIZE,VolumeType=$VOLUME_TYPE}" \
        --instance-initiated-shutdown-behavior stop \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG_NAME}]" \
        --query 'Instances[].InstanceId' --output text)
    echo "instances: $ids"

    say "Waiting until all $NODE_COUNT instances pass their status checks"
    # shellcheck disable=SC2086
    aws ec2 wait instance-status-ok --instance-ids $ids

    step_nodes "$ids"
}

###############################################################################
# STEP: nodes — write nodes.env from the running instances
#
# node0 is simply the lexicographically first instance id, so the choice of
# initiator is stable across invocations.
###############################################################################
step_nodes() {
    local ids="${1:-}"
    say "Collecting node addresses"
    local q='Reservations[].Instances[?State.Name==`running`].[InstanceId,PrivateIpAddress,PublicIpAddress]'
    local rows
    if [ -n "$ids" ]; then
        # shellcheck disable=SC2086
        rows=$(aws ec2 describe-instances --instance-ids $ids --query "$q" --output text | sort)
    else
        rows=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=$TAG_NAME" \
               "Name=instance-state-name,Values=running" --query "$q" --output text | sort)
    fi
    [ -n "$rows" ] || die "No running instances tagged $TAG_NAME."

    local priv pub
    priv=$(awk '{print $2}' <<< "$rows" | tr '\n' ' ' | sed 's/ $//')
    pub=$(awk  '{print $3}' <<< "$rows" | tr '\n' ' ' | sed 's/ $//')
    printf '%s\n' "$rows" | awk '{printf "  %-20s private=%-16s public=%s\n", $1, $2, $3}'

    cat > "$NODES_FILE" <<EOF
# Generated by clickbench-cluster.sh. First entry is the initiator (node0).
CB_CLUSTER="$CLUSTER_NAME"
CB_NODE_COUNT="$NODE_COUNT"
CB_NODES="$priv"
CB_PUBLIC="$pub"
CB_SSH_USER="$SSH_USER"
CB_SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
CB_WORK_DIR="$NODE_WORK_DIR"
EOF
    say "Wrote $NODES_FILE"
    cat "$NODES_FILE"
}

###############################################################################
# STEP: gen — write the clickhouse-cluster ClickBench system directory
#
# This is the part worth reading. It produces, locally, a directory that obeys
# the same contract as clickhouse/: install, start, check, stop, load, query,
# data-size, benchmark.sh, create*.sql, queries.sql, template.json — plus the
# ./flush-caches hook the driver offers for "caches the kernel can't drop",
# which here means "page cache on the other two nodes".
#
# It needs the single-node ClickBench checkout for create.sql and queries.sql,
# so run clickbench-local.sh fetch first (or set CB_DIR).
###############################################################################
step_gen() {
    local cb_dir="${CB_DIR:-$NODE_WORK_DIR/ClickBench}"
    [ -d "$cb_dir/clickhouse" ] || die "No ClickBench checkout at $cb_dir — run ./clickbench-local.sh fetch, or set CB_DIR."

    say "Generating $GEN_DIR"
    mkdir -p "$GEN_DIR"

    # ---- unchanged from the single-node entry -----------------------------
    cp "$cb_dir/clickhouse/queries.sql"  "$GEN_DIR/queries.sql"
    cp "$cb_dir/clickhouse/query"        "$GEN_DIR/query"
    cp "$cb_dir/clickhouse/template.json" "$GEN_DIR/template.json"

    # The local shard table: the single-node DDL, renamed. Identical engine,
    # identical primary key, identical settings — that is the whole point.
    sed 's/CREATE OR REPLACE TABLE hits$/CREATE OR REPLACE TABLE hits_local/' \
        "$cb_dir/clickhouse/create.sql" > "$GEN_DIR/create_local.sql"
    grep -q 'hits_local' "$GEN_DIR/create_local.sql" || die "Rename of the CREATE TABLE failed — check upstream create.sql."

    cat > "$GEN_DIR/create_distributed.sql" <<'EOF'
-- The queries in queries.sql say "FROM hits" and are used verbatim, so the
-- distributed table has to carry that name. The sharding key is only used for
-- writes through this table; we never do that (each shard ingests its own
-- files locally), it is here because Distributed requires one.
CREATE OR REPLACE TABLE hits AS hits_local
ENGINE = Distributed(CB_CLUSTER_NAME, default, hits_local, rand());
EOF

    cat > "$GEN_DIR/cluster-lib.sh" <<'EOF'
#!/bin/bash
# Shared by the scripts in this directory. nodes.env is generated per
# deployment (it holds IP addresses) and is not part of the repository.
. "$(dirname "${BASH_SOURCE[0]}")/nodes.env"

# Run a command on one node. The initiator ssh's to itself too, so every
# fan-out is uniform and there is no "am I the local one" special case.
on_node() {
    local host="$1"; shift
    ssh $CB_SSH_OPTS "$CB_SSH_USER@$host" "$@"
}

# Run the same command on every node, in parallel, and fail if any fails.
on_all_nodes() {
    local host pids=() rc=0
    for host in $CB_NODES; do
        on_node "$host" "$@" &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p" || rc=1; done
    return $rc
}
EOF

    cat > "$GEN_DIR/benchmark.sh" <<'EOF'
#!/bin/bash
# The dataset is fetched per-shard by ./install (each node downloads only the
# files it will own), so the shared single-node downloader is disabled here.
export BENCH_DOWNLOAD_SCRIPT=""
exec ../lib/benchmark-common.sh
EOF

    cat > "$GEN_DIR/install" <<'EOF'
#!/bin/bash
# Installs ClickHouse on every node, writes the cluster config, and gives each
# node the third of the parquet files it will own. Not part of any timed
# window (the driver times ./load only).
set -e
. ./cluster-lib.sh

i=0
pids=()
for host in $CB_NODES; do
    on_node "$host" "sudo bash -s -- $i $CB_NODE_COUNT $CB_CLUSTER $CB_WORK_DIR $CB_NODES" \
        < ./node-install.sh &
    pids+=($!)
    i=$((i + 1))
done
rc=0
for p in "${pids[@]}"; do wait "$p" || rc=1; done
exit $rc
EOF

    cat > "$GEN_DIR/node-install.sh" <<'EOF'
#!/bin/bash
# Runs as root on each cluster node, fed to `sudo bash -s` over ssh by ./install.
# Args: <node-index> <node-count> <cluster-name> <work-dir> <node-ip>...
set -e
IDX="$1"; N="$2"; CLUSTER="$3"; WORK_DIR="$4"; shift 4
NODES="$*"
PRIVATE_IP=$(hostname -I | awk '{print $1}')

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y wget curl jq

# Same installation path as the single-node entry: the official binary.
if [ ! -x /usr/bin/clickhouse ]; then
    cd /tmp
    curl -fsSL --retry 10 --retry-delay 60 --retry-all-errors https://clickhouse.com/ | sh
    ./clickhouse install --noninteractive
fi

# Identical to clickhouse/install: eager loading so the cold timer measures the
# query and not the part loader.
mkdir -p /etc/clickhouse-server/config.d
tee /etc/clickhouse-server/config.d/eager_load.yaml >/dev/null <<'YAML'
async_load_databases: false
merge_tree:
  primary_key_lazy_load: 0
  columns_and_secondary_indices_sizes_lazy_calculation: 0
YAML

# The only real configuration change: the cluster definition, plus listening on
# the private address so the shards can reach each other. XML rather than YAML
# because <shard> repeats, and repeated keys are where the YAML-to-XML
# conversion gets subtle.
{
    echo '<clickhouse>'
    echo '    <listen_host>127.0.0.1</listen_host>'
    echo "    <listen_host>${PRIVATE_IP}</listen_host>"
    echo '    <remote_servers>'
    echo "        <${CLUSTER}>"
    for h in $NODES; do
        echo "            <shard><replica><host>${h}</host><port>9000</port></replica></shard>"
    done
    echo "        </${CLUSTER}>"
    echo '    </remote_servers>'
    echo '</clickhouse>'
} > /etc/clickhouse-server/config.d/cluster.xml

# This node's share of the dataset: files where index % N == IDX, i.e. 34/33/33
# of the 100 partitioned parquet files. Straight into user_files, so the server
# can read them with no symlink and no traversal problem.
DEST=/var/lib/clickhouse/user_files
mkdir -p "$DEST" "$WORK_DIR"
cd "$DEST"
seq 0 99 | awk -v n="$N" -v i="$IDX" '($1 % n) == i' \
    | xargs -P16 -I{} wget --continue --quiet \
        https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet
chown -R clickhouse:clickhouse "$DEST"
echo "node $IDX: $(ls -1 "$DEST"/hits_*.parquet | wc -l) parquet files, $(du -sh "$DEST" | cut -f1)"

clickhouse start || true
EOF

    cat > "$GEN_DIR/start" <<'EOF'
#!/bin/bash
set -e
. ./cluster-lib.sh
on_all_nodes 'sudo clickhouse start'
EOF

    cat > "$GEN_DIR/stop" <<'EOF'
#!/bin/bash
. ./cluster-lib.sh
on_all_nodes 'sudo clickhouse stop' || true
EOF

    cat > "$GEN_DIR/check" <<'EOF'
#!/bin/bash
# Readiness for the whole cluster, not just the initiator: clusterAllReplicas
# touches every shard and throws if one is unreachable, so a node that failed
# to come back cannot be silently benchmarked away.
set -e
. ./cluster-lib.sh
n=$(clickhouse-client --query "SELECT count() FROM clusterAllReplicas('$CB_CLUSTER', system.one)")
[ "$n" = "$CB_NODE_COUNT" ]
EOF

    cat > "$GEN_DIR/flush-caches" <<'EOF'
#!/bin/bash
# The driver drops the page cache on the machine it runs on and then calls this
# hook. Without it, two thirds of the cluster would answer the "cold" run from
# a warm page cache.
. ./cluster-lib.sh
on_all_nodes 'sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null' || true
EOF

    cat > "$GEN_DIR/load" <<'EOF'
#!/bin/bash
# Timed by the driver.
#
# Each shard ingests the files it already holds, all three in parallel. Loading
# 100 files through one node would push two thirds of the rows over the network
# and measure the interconnect instead of the database; every distributed entry
# in ClickBench (Databricks, Snowflake, Redshift) loads in parallel too.
set -e
. ./cluster-lib.sh

for host in $CB_NODES; do
    clickhouse-client -h "$host" < create_local.sql &
done
wait

sed "s/CB_CLUSTER_NAME/$CB_CLUSTER/" create_distributed.sql | clickhouse-client

# file() is evaluated server-side, so this reads the parquet files that live on
# $host — no data crosses the network during the load.
threads=$(( $(nproc) / 4 ))
for host in $CB_NODES; do
    clickhouse-client -h "$host" \
        --query "INSERT INTO hits_local SELECT * FROM file('hits_*.parquet')" \
        --max-insert-threads "$threads" &
done
wait

# The driver's own sync only covers the initiator.
on_all_nodes 'sync'
EOF

    cat > "$GEN_DIR/data-size" <<'EOF'
#!/bin/bash
set -e
. ./cluster-lib.sh
clickhouse-client --query "
    SELECT sum(total_bytes)
    FROM clusterAllReplicas('$CB_CLUSTER', system.tables)
    WHERE database = 'default' AND name = 'hits_local'"
EOF

    cat > "$GEN_DIR/.gitignore" <<'EOF'
# Deployment-specific: generated by clickbench-cluster.sh, holds private IPs.
nodes.env
log
result.csv
EOF

    cat > "$GEN_DIR/README.md" <<EOF
# ClickHouse, $NODE_COUNT-node cluster

Self-managed ClickHouse on $NODE_COUNT × \`$MACHINE\` EC2 instances
($VOLUME_SIZE GB $VOLUME_TYPE, Ubuntu 24.04, one availability zone, cluster
placement group). Same binary, same \`create.sql\`, same \`queries.sql\` as the
single-node \`clickhouse\` entry — the result is directly comparable to
\`clickhouse/results/<date>/$MACHINE.json\`.

## Topology

$NODE_COUNT shards, no replicas, no ClickHouse Keeper (Keeper is only required
for ReplicatedMergeTree and \`ON CLUSTER\` DDL; the DDL here is sent to each
shard over the native protocol). Each node holds a \`hits_local\` MergeTree
table with the DDL of the single-node entry; the initiator additionally holds
\`hits\`, a \`Distributed\` table over the three shards. The 43 queries are
unmodified — none of them join or use subqueries, so \`FROM hits\` resolves to
the distributed table.

## Deviations from the single-node entry, and why

* **Parallel load.** Each shard ingests the ~33 parquet files it holds. Routing
  the whole dataset through one node would measure the interconnect.
* **Restart on all nodes between queries.** \`stop\`/\`start\` and the
  \`flush-caches\` hook fan out over ssh, so the cold runs are true cold runs on
  every shard, as the rules require.
* **Data size** is the sum over shards (\`clusterAllReplicas\`).
* **Not run by \`run-benchmark.sh\`.** That launches a single instance; this
  entry is operator-driven. \`nodes.env\` (node addresses, ssh settings) is
  generated per deployment and gitignored.

## Running it

\`\`\`
# on the initiator, with nodes.env in place and passwordless ssh to all nodes
./benchmark.sh
\`\`\`
EOF

    # create_distributed.sql keeps a placeholder so the file is deployment
    # independent; ./load substitutes the cluster name at run time.
    chmod +x "$GEN_DIR"/{install,start,stop,check,load,query,data-size,benchmark.sh,flush-caches,cluster-lib.sh}

    say "Generated files"
    ls -la "$GEN_DIR"
    echo
    echo "Read them now — this directory is what the pull request will contain."
}

###############################################################################
# STEP: bootstrap — ssh trust, the checkout, and the generated dir on node0
###############################################################################
step_bootstrap() {
    load_nodes
    [ -d "$GEN_DIR" ] || die "Run the gen step first."
    local node0 pub0
    node0=$(awk '{print $1}' <<< "$CB_NODES")
    pub0=$(awk '{print $1}' <<< "$CB_PUBLIC")

    say "Waiting for ssh on all nodes"
    local h
    for h in $CB_PUBLIC; do
        until ssh_pub "$h" true 2>/dev/null; do sleep 5; done
        echo "  $h ok"
    done

    say "Giving node0 the key so it can drive the other nodes"
    # It also ssh's to itself — that keeps every fan-out loop uniform.
    scp $SSH_OPTS -i "$SSH_KEY" "$SSH_KEY" "$SSH_USER@$pub0:.ssh/id_rsa"
    ssh_pub "$pub0" "chmod 600 .ssh/id_rsa"
    ssh_pub "$pub0" "for h in $CB_NODES; do ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \$h true && echo \"  -> \$h ok\"; done"

    say "Cloning ClickBench on node0 into $NODE_WORK_DIR"
    ssh_pub "$pub0" "sudo mkdir -p $NODE_WORK_DIR && sudo chown $SSH_USER:$SSH_USER $NODE_WORK_DIR && sudo chmod 755 $NODE_WORK_DIR && \
        { [ -d $NODE_WORK_DIR/ClickBench/.git ] || git clone --depth 1 https://github.com/ClickHouse/ClickBench.git $NODE_WORK_DIR/ClickBench; }"

    say "Pushing $SYSTEM/ to node0"
    scp $SSH_OPTS -i "$SSH_KEY" -r "$GEN_DIR" "$SSH_USER@$pub0:$NODE_WORK_DIR/ClickBench/"
    scp $SSH_OPTS -i "$SSH_KEY" "$NODES_FILE" "$SSH_USER@$pub0:$NODE_WORK_DIR/ClickBench/$SYSTEM/nodes.env"
    ssh_pub "$pub0" "cd $NODE_WORK_DIR/ClickBench/$SYSTEM && chmod +x install start stop check load query data-size benchmark.sh flush-caches && ls -la"

    say "Node0 is $pub0 — shell into it with: $0 ssh0"
}

###############################################################################
# STEP: run — the measured run, detached on node0
#
# nohup + setsid so a dropped ssh connection does not kill a two-hour run.
# The preamble mirrors what cloud-init.sh.in writes upstream, so the log has
# the same shape and the results step can parse it the same way.
###############################################################################
step_run() {
    load_nodes
    local pub0; pub0=$(awk '{print $1}' <<< "$CB_PUBLIC")
    local dir="$NODE_WORK_DIR/ClickBench/$SYSTEM"

    say "Starting the run on node0 ($pub0)"
    ssh_pub "$pub0" "cd $dir && \
        { echo -n 'System name: ';  jq -r    '.system'      template.json; \
          echo -n 'Proprietary: ';  jq -r    '.proprietary' template.json; \
          echo -n 'Tuned: ';        jq -r    '.tuned'       template.json; \
          echo -n 'Tags: ';         jq -c -r '.tags'        template.json; \
          echo 'Machine: $MACHINE'; \
          echo 'Cluster size: $NODE_COUNT'; } > log && \
        setsid nohup ./benchmark.sh >> log 2>&1 < /dev/null & \
        sleep 2; echo started"
    echo
    echo "Follow it with:  $0 status"
    echo "It is done when the log has 43 '[t1, t2, t3],' lines and a 'Data size:' line."
}

step_status() {
    load_nodes
    local pub0; pub0=$(awk '{print $1}' <<< "$CB_PUBLIC")
    local dir="$NODE_WORK_DIR/ClickBench/$SYSTEM"
    say "Progress"
    ssh_pub "$pub0" "cd $dir && \
        echo \"query result lines: \$(grep -cE '^\\[' log) / 43\" && \
        grep -E '^(Load time|Data size|Concurrent )' log || true; \
        echo '--- tail ---'; tail -n 15 log"
}

step_ssh0() {
    load_nodes
    local pub0; pub0=$(awk '{print $1}' <<< "$CB_PUBLIC")
    exec ssh $SSH_OPTS -i "$SSH_KEY" "$SSH_USER@$pub0"
}

step_fetch_log() {
    load_nodes
    local pub0; pub0=$(awk '{print $1}' <<< "$CB_PUBLIC")
    scp $SSH_OPTS -i "$SSH_KEY" "$SSH_USER@$pub0:$NODE_WORK_DIR/ClickBench/$SYSTEM/log" "$LOG"
    scp $SSH_OPTS -i "$SSH_KEY" "$SSH_USER@$pub0:$NODE_WORK_DIR/ClickBench/$SYSTEM/result.csv" "$BASE_DIR/cluster-result.csv" 2>/dev/null || true
    say "Fetched $LOG ($(grep -cE '^\[' "$LOG") query lines)"
}

###############################################################################
# STEP: results — the log becomes the result JSON
#
# Same parser as clickbench-local.sh, with two differences that matter for
# publishing: machine is a real EC2 instance type, and cluster_size is 3.
###############################################################################
step_results() {
    [ -s "$LOG" ] || die "No log at $LOG — run the fetch-log step first."
    say "Building the result JSON"
    local load_time data_size qps err_ratio results n date_dir out
    load_time=$(grep -m1 '^Load time: ' "$LOG" | awk '{printf "%.0f", $3}')
    data_size=$(grep -m1 '^Data size: ' "$LOG" | awk '{print $3}')
    qps=$(grep -m1 '^Concurrent QPS: ' "$LOG" | awk '{print $3}')
    err_ratio=$(grep -m1 '^Concurrent error ratio: ' "$LOG" | awk '{print $4}')
    : "${qps:=null}" "${err_ratio:=null}"
    [ -n "$load_time" ] || die "No 'Load time:' line in the log — the run did not get that far."

    results=$(grep -E '^\[' "$LOG" | sed 's/,$//' | jq -s .)
    n=$(jq 'length' <<< "$results")
    [ "$n" -eq 43 ] || warn "Got $n query rows, expected 43 (partial run?)"

    date_dir=$(date -u +%Y%m%d)
    mkdir -p "$GEN_DIR/results/$date_dir"
    out="$GEN_DIR/results/$date_dir/$MACHINE.json"

    jq -n \
        --slurpfile t "$GEN_DIR/template.json" \
        --arg   date "$(date -u +%Y-%m-%d)" \
        --arg   machine "$MACHINE" \
        --argjson cluster "$NODE_COUNT" \
        --argjson load "$load_time" \
        --argjson size "$data_size" \
        --argjson qps "$qps" \
        --argjson err "$err_ratio" \
        --argjson res "$results" \
        '{
            system:                 $t[0].system,
            date:                   $date,
            machine:                $machine,
            cluster_size:           $cluster,
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

    jq empty "$out" && say "Wrote $out"
    jq 'del(.result)' "$out"
    echo
    echo "The dashboard will label this \"ClickHouse (${NODE_COUNT}×${MACHINE})\"."
}

###############################################################################
# STEP: submit — validate and print the contribution checklist
###############################################################################
step_submit() {
    local cb_dir="${CB_DIR:-$NODE_WORK_DIR/ClickBench}"
    say "Validating the result file"
    if [ -f "$cb_dir/validate-results.py" ]; then
        # The validator walks <system>/results/*/*.json relative to a root, so
        # stage the generated dir inside a checkout to check it.
        rm -rf "$cb_dir/$SYSTEM"
        cp -r "$GEN_DIR" "$cb_dir/$SYSTEM"
        rm -f "$cb_dir/$SYSTEM/nodes.env"
        (cd "$cb_dir" && python3 validate-results.py .) || warn "Validator reported problems."
    else
        warn "No checkout at $cb_dir — skipping validation."
    fi

    cat <<EOF

$(printf '\033[1;36m==> Checklist for the pull request\033[0m')

  1. Fork ClickHouse/ClickBench, branch off main.
  2. Copy $GEN_DIR to <fork>/$SYSTEM
     (nodes.env is gitignored — do not commit node addresses).
  3. The directory must contain, per the contribution rules:
       benchmark.sh create_local.sql create_distributed.sql queries.sql
       install start stop check load query data-size flush-caches
       template.json README.md results/$(date -u +%Y%m%d)/$MACHINE.json
  4. Run ./validate-results.py . at the repo root — it must exit 0.
  5. Optionally regenerate the site: ./generate-results.sh (CI does it anyway).
  6. In the PR description, state explicitly:
       - $NODE_COUNT × $MACHINE, $VOLUME_SIZE GB $VOLUME_TYPE, Ubuntu 24.04, one AZ,
         cluster placement group;
       - $NODE_COUNT shards / no replicas / no Keeper; unmodified queries against a
         Distributed table;
       - each shard loads its own third of the partitioned parquet files;
       - true cold runs: the whole cluster is restarted and the page cache is
         dropped on every node before each first run;
       - the run is operator-driven because run-benchmark.sh only launches a
         single instance. Offer to extend the automation if they want it.
  7. Expect review questions about the parallel load and about cluster_size
     semantics for a self-managed system — no other self-hosted entry uses it.

EOF
}

###############################################################################
# STEP: terminate — stop paying
###############################################################################
step_terminate() {
    say "Instances tagged $TAG_NAME"
    local ids
    ids=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=$TAG_NAME" \
          "Name=instance-state-name,Values=running,stopped" \
          --query 'Reservations[].Instances[].InstanceId' --output text)
    [ -n "$ids" ] || { echo "None."; return 0; }
    echo "$ids"
    read -r -p "Terminate these instances? This deletes their volumes. [type: yes] " answer
    [ "$answer" = "yes" ] || die "Aborted."
    # shellcheck disable=SC2086
    aws ec2 terminate-instances --instance-ids $ids --output table
    echo
    echo "The security group and placement group are kept for the next run. Remove with:"
    echo "    aws ec2 delete-placement-group --group-name $PG_NAME"
    echo "    aws ec2 delete-security-group  --group-name $SG_NAME"
}

###############################################################################
# Dispatcher
###############################################################################
step_help() {
    sed -n 's/^# \{0,1\}//p' <<'EOF'
# Steps:
#   preflight    awscli, credentials, ssh key, cost estimate
#   provision    security group, placement group, 3 × EC2, write nodes.env
#   nodes        re-read the running instances into nodes.env
#   gen          generate the clickhouse-cluster system directory here
#   bootstrap    ssh trust, clone ClickBench on node0, push the directory
#   run          start the measured run, detached, on node0
#   status       progress and log tail
#   ssh0         interactive shell on the initiator
#   fetch-log    bring the log (and result.csv) back here
#   results      log -> clickhouse-cluster/results/<date>/<machine>.json
#   submit       validate + pull request checklist
#   terminate    terminate the instances (asks for confirmation)
#
# Useful overrides:
#   MACHINE=c6a.8xlarge        instance type (also the result's machine field)
#   NODE_COUNT=5               shards
#   KEY_NAME / SSH_KEY         EC2 key pair and the matching .pem
#   CB_DIR=/path/to/ClickBench checkout used by gen and submit
#   BENCH_TRIES / BENCH_CONCURRENT_DURATION are set on node0, not here:
#       ssh0, then: BENCH_TRIES=1 BENCH_CONCURRENT_DURATION=0 ./benchmark.sh
EOF
}

case "${1:-help}" in
    preflight)      step_preflight ;;
    provision)      step_provision ;;
    nodes)          step_nodes ;;
    gen)            step_gen ;;
    bootstrap)      step_bootstrap ;;
    run)            step_run ;;
    status)         step_status ;;
    ssh0)           step_ssh0 ;;
    fetch-log)      step_fetch_log ;;
    results)        step_results ;;
    submit)         step_submit ;;
    terminate)      step_terminate ;;
    help|-h|--help) step_help ;;
    *)              die "Unknown step '$1' — try: $0 help" ;;
esac
