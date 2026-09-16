# SeaweedFS Memory Pressure and OOM Diagnostics (September 2026)

## Summary
During pipeline batch runs, particularly during concurrent Spark parquet writes to S3, the `np_seaweedfs` container was intermittently terminated by the Linux kernel out-of-memory killer (exit code 137).

## Diagnosis & Evidence
- Kernel messages recorded via `dmesg` indicated that the `weed` process exceeded its container cgroup limit (`MEM_S3=768m`), reaching approximately 860 MB RSS before termination.
- Go runtime memory behavior: Go's memory allocator and garbage collector scale allocation to available host memory unless bounded by runtime constraints. Under bursty object uploads, heap allocations outpaced standard garbage collection intervals.

## Root Cause Analysis
SeaweedFS server (`weed server -s3 -filer`) was configured with a container limit of 768 MB without an explicit `GOMEMLIMIT` environment variable configured. In the absence of a soft memory limit, the Go runtime did not trigger GC aggressively enough to remain within the 768 MB container boundary.

## Mitigation Strategy
1. Configure `GOMEMLIMIT=650MiB` on the `seaweedfs` service definition in `docker-compose.yml` to ensure the Go GC triggers prior to hitting the container cgroup limit.
2. If peak memory requirements during high-concurrency ingestion exceed 768 MB, reallocate memory from lower-utilization services within the global 5.5 GB VM budget.
