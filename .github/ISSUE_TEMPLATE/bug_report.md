---
name: Bug Report
about: Something isn't working as expected
title: "[BUG] "
labels: ["bug", "needs-triage"]
assignees: []
---

## What happened?

A clear, concise description of what went wrong.

## What did you expect to happen?

A clear, concise description of what you expected to happen.

## How can we reproduce this?

**Steps to reproduce:**

1. Run `porter query "..."` 
2. See error: `...`

**Or share a minimal example:**

```sql
-- Your query here
SELECT * FROM table WHERE condition;
```

## Environment

- **Porter version:** `porter --version`
- **OS:** macOS/Linux/Windows
- **Go version:** `go version`
- **DuckDB version:** (if relevant)

## Additional context

Any other context, logs, or screenshots that might help. Don't worry if you're not sure what's relevant—we'll figure it out together.

## Checklist

- [ ] I've searched existing issues to avoid duplicates
- [ ] I've tried the latest version of Porter
- [ ] I've included enough detail to help debug this

---

> **Remember:** Don't be afraid of making a mistake. We do it all the time. Just look at our PRs. 