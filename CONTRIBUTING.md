# Contributing to Porter

> Have some fun. Don’t be afraid of making a mistake.
> I do it all the time. Just look at my PRs.

---

Porter is a project built on restraint, clarity, and speed—but that doesn’t mean the process has to be uptight. Whether you’re fixing a typo, adding a backend, or building something weird and beautiful on top of Flight SQL, you’re welcome here.

You don’t need to be an expert. You just need to care.

---

## Community Principles

Porter is small, focused software. The community around it should feel the same way: calm, curious, and collaborative.

* **Be kind.** Especially when reviewing, explaining, or disagreeing.
* **Be concise.** We value clarity over volume.
* **Be honest.** Don’t fake confidence. Nobody here expects you to know everything.
* **Be generous.** Share what you learn. Credit others. Ask good questions.

This isn’t corporate open source. It’s a shared effort by people who want fast, secure systems that don’t make your brain hurt.

---

## Getting Started

1. **Fork the repo**

2. **Clone and set up**:

   ```bash
   git clone https://github.com/TFMV/porter.git
   cd porter
   go mod download
   make build  # or: go build -o porter ./cmd/server
   ```

3. **Run tests**:

   ```bash
   make test
   ```

4. **Try a real query**:

   ```bash
   ./porter query "SELECT 42 as answer"
   ```

If that worked, you're good to go.

---

## Making Changes

1. **Create a branch**:

   ```bash
   git checkout -b feature/my-thing
   ```

2. **Build, test, commit**:

   ```bash
   make test
   git commit -m "feat: add my thing"
   ```

3. **Push and open a pull request**.

No one will yell at you for imperfect commits or open questions. That’s what reviews are for.

---

## Code Style

* Use `gofmt`, `go vet`, and your best judgment.
* Write code you’d want to inherit.
* Add comments where future-you might say “wtf is this?”
* Structure code like it matters. It does.

---

## Pull Request Checklist

Before submitting:

* [ ] Tests pass
* [ ] Docs are updated (if needed)
* [ ] You’ve explained **why** the change matters
* [ ] You’ve had at least one moment of joy while working on it

If you’re not sure whether it’s “good enough,” open it anyway. We’ll figure it out together.

---

## Benchmarking Contributions

If you're working on `porter bench`, great! It's one of the most fun parts of the codebase. Check out `pkg/benchmark/` and follow the patterns there.

You can:

* Add new TPC-H queries
* Build new suites (e.g. TPC-DS)
* Improve output formatting (`--json`, `--arrow`, etc.)

Let us know if you're stuck—we’ll help.

---

## New Here? Start Here.

Look for issues tagged `good first issue` or `help wanted`. Or open a new one with your idea.

You can also:

* Fix typos in docs
* Add backend support
* Refactor little things
* Improve error messages or CLI UX

We welcome contributions of **any size**. Really.

---

## Philosophy

Porter doesn’t try to be a platform.
It tries to do one thing well: serve SQL queries—fast, secure, and without drama.

If you’re aligned with that, you’re already part of the project.

---

## Final Note

Thanks for considering a contribution. Porter exists because people like you care about software that’s fast, clear, and quiet. Your effort—however small—makes it better.

> Now go make something weird.
> If it breaks, we’ll fix it together.
