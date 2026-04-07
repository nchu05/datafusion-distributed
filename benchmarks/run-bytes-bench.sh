#!/usr/bin/env bash
# Bytes-transferred benchmark for PartialReduce optimization.
# Runs high-cardinality GROUP BY queries to measure network byte reduction.
#
# Usage:
#   WORKERS=8 ./benchmarks/run-bytes-bench.sh --threads 2 --dataset tpch_sf1
#
# Run on two branches and the second run auto-compares with the first.

set -e

WORKERS=${WORKERS:-8}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

BYTES_QUERIES="bytes_q1,bytes_q2,bytes_q3,bytes_q4,bytes_q5,bytes_q6,bytes_q7,bytes_q8,bytes_q9,bytes_q10,bytes_q11,bytes_q12,bytes_q13,bytes_q14,bytes_q15,bytes_q16,bytes_q17,bytes_q18,bytes_q19,bytes_q20,bytes_q21,bytes_q22"

if [ "$WORKERS" == "0" ]; then
  cargo run -p datafusion-distributed-benchmarks --release -- run --collect-metrics --query "$BYTES_QUERIES" "$@"
  exit
fi

cleanup() {
  echo "Cleaning up processes..."
  for i in $(seq 1 $((WORKERS))); do
    kill "%$i" 2>/dev/null || true
  done
}

wait_for_port() {
  local port=$1
  local timeout=30
  local elapsed=0
  while ! nc -z localhost "$port" 2>/dev/null; do
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "Timeout waiting for port $port"
      return 1
    fi
    sleep 0.1
    elapsed=$((elapsed + 1))
  done
  echo "Port $port is ready"
}

cargo build -p datafusion-distributed-benchmarks --release

trap cleanup EXIT INT TERM
for i in $(seq 0 $((WORKERS-1))); do
  "$SCRIPT_DIR"/../target/release/dfbench run --spawn $((8000+i)) "$@" &
done

echo "Waiting for worker ports to be ready..."
for i in $(seq 0 $((WORKERS-1))); do
  wait_for_port $((8000+i))
done

"$SCRIPT_DIR"/../target/release/dfbench run \
  --workers $(seq -s, 8000 $((8000+WORKERS-1))) \
  --collect-metrics \
  --query "$BYTES_QUERIES" \
  "$@"
