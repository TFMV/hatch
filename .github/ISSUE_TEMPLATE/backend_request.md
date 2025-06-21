---
name: Backend Request
about: Request or contribute support for a new database backend
title: "[BACKEND] "
labels: ["backend", "enhancement", "needs-triage"]
assignees: []
---

## What backend are you requesting?

**Database:** PostgreSQL/MySQL/SQLite/etc.

**Why this backend?**
A clear, concise explanation of why this backend would be valuable for Porter users.

## Current status

**Does this backend have Flight SQL support?**
- [ ] Native Flight SQL support
- [ ] Third-party Flight SQL adapter
- [ ] No Flight SQL support (would need custom implementation)

**Existing implementations:**
- Links to any existing Flight SQL adapters
- Related projects or libraries

## Implementation approach

**How do you plan to implement this?**
- [ ] Use existing Flight SQL adapter
- [ ] Create custom Flight SQL implementation
- [ ] Extend Porter's backend abstraction
- [ ] Other: _____

**Technical considerations:**
- Connection pooling requirements
- Transaction support needs
- Authentication mechanisms
- Performance characteristics

## Use cases

**What scenarios would this backend support?**
- [ ] OLTP workloads
- [ ] OLAP/analytics
- [ ] Embedded applications
- [ ] Cloud-native deployments
- [ ] Other: _____

## Additional context

Any other details about the backend, implementation challenges, or user needs.

## Checklist

- [ ] I've researched existing Flight SQL support for this backend
- [ ] This aligns with Porter's focus on fast, secure SQL serving
- [ ] I understand the implementation complexity
- [ ] I'm open to discussing technical details

---

> **Remember:** Porter is built on restraint, clarity, and speed. New backends should enhance these qualities, not compromise them. 