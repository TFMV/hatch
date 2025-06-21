---
name: Benchmark Request
about: Request or contribute performance benchmarks
title: "[BENCH] "
labels: ["benchmark", "performance", "needs-triage"]
assignees: []
---

## What are you benchmarking?

A clear description of what performance aspect you want to measure or improve.

## Current performance

**What's the current behavior?**
- Query execution time: `X ms`
- Throughput: `Y rows/sec`
- Memory usage: `Z MB`

**How are you measuring it?**
```bash
# Your benchmark command
porter bench --query "SELECT ..."
```

## Target performance

**What performance are you aiming for?**
- Target execution time: `X ms`
- Target throughput: `Y rows/sec`
- Target memory usage: `Z MB`

## Benchmark details

**Query patterns:**
- [ ] TPC-H queries
- [ ] TPC-DS queries
- [ ] Custom workload
- [ ] Flight SQL protocol throughput
- [ ] Memory allocation patterns
- [ ] Connection pooling performance

**Data characteristics:**
- Dataset size: `X GB`
- Row count: `Y million`
- Column types: `text, int, float, etc.`

## Implementation approach

**How do you plan to measure this?**
- [ ] Use existing `porter bench` framework
- [ ] Create new benchmark in `pkg/benchmark/`
- [ ] Use external tools (specify)
- [ ] Other: _____

## Additional context

Any other details about your benchmarking goals or constraints.

## Checklist

- [ ] I've checked existing benchmarks in `pkg/benchmark/`
- [ ] I understand Porter's focus on speed and clarity
- [ ] I'm open to collaborating on implementation

---

> **Remember:** Benchmarking is one of the most fun parts of the codebase. If you're stuck, we'll help. Check out `pkg/benchmark/` and follow the patterns there. 