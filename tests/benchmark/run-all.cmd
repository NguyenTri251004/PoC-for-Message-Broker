@echo off
REM Full benchmark: synchronous baseline vs RabbitMQ asynchronous.
REM IMPORTANT: run from the repository root.
set BENCH_DURATION=20s
set BENCH_WARMUP=5s
set BENCH_WARMUP_VUS=5
set BENCH_ITERATIONS=3
set BENCH_DRAIN_TIMEOUT_MS=60000
node tests/benchmark/run-benchmark.mjs
