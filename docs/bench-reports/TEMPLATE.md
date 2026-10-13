# Benchmark: <machine> (<date>)

Run with [the benchmarking workflow](../benchmarking.md). Baseline for comparison:
[2026-10-12-linux-core-ultra-7-155h.md](2026-10-12-linux-core-ultra-7-155h.md).

## Machine and settings

```
<paste env.txt>
```

Anything else running, power and thermal state:

## Execution ceiling (4a, 4b)

| Test | Threads | execute+commit TPS |
|---|---|---|
| One validator alone, 5000-txn blocks | 4 | |
| One validator alone, 5000-txn blocks | 8 | |
| 4 concurrent, 1000-txn blocks (per validator) | 2 / 3 / 4 / 5 | |

## End to end (4c)

<paste results.md; one table per run, including repeats>

## Summary

- Sustained TPS (all committed, p50 finality under ~3 s):
- Peak committed TPS (with its p50 finality):
- Compared with the baseline:
- Observations:
