# Contributing to Porter

Thank you for your interest in contributing to Porter! This document provides guidelines for contributing to the project, from community standards to technical development.

## Community Guidelines

### Code of Conduct

Porter is committed to providing a welcoming and inclusive environment for all contributors. We follow these principles:

- **Be Respectful**: Treat all contributors with respect and kindness
- **Be Inclusive**: Welcome contributors from all backgrounds and experience levels
- **Be Constructive**: Provide helpful, constructive feedback
- **Be Patient**: Remember that everyone is learning and growing
- **Be Collaborative**: Work together to improve the project

### Communication Standards

#### In Issues and Discussions
- Use clear, descriptive titles and descriptions
- Be specific about problems, solutions, and expected outcomes
- Reference related issues or pull requests when applicable
- Use appropriate labels and milestones
- Respond to feedback and questions promptly

#### In Code Reviews
- Be constructive and specific in feedback
- Focus on the code, not the person
- Explain the reasoning behind suggestions
- Acknowledge good work and improvements
- Use a respectful tone even when pointing out issues

#### In Pull Requests
- Provide clear descriptions of changes
- Include tests for new functionality
- Update documentation as needed
- Respond to review comments promptly
- Keep commits focused and well-described

### Reporting Issues

When reporting issues:

1. **Search first**: Check if the issue has already been reported
2. **Be specific**: Include steps to reproduce, expected vs actual behavior
3. **Include context**: OS, Go version, Porter version, relevant logs
4. **Use templates**: Follow the provided issue templates
5. **Be patient**: Maintainers are volunteers with limited time

### Getting Help

- **Documentation**: Check the README and docs/ directory first
- **Issues**: Search existing issues for similar problems
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Community**: Be respectful when asking for help

## Development Setup

### Prerequisites

- **Go 1.24+**: Required for building and testing
- **Git**: For version control
- **DuckDB**: Automatically handled via Go bindings
- **Make**: For build automation (optional but recommended)

### Local Development Environment

1. **Clone the repository**:
   ```bash
   git clone https://github.com/TFMV/porter.git
   cd porter
   ```

2. **Install dependencies**:
   ```bash
   go mod download
   ```

3. **Build Porter**:
   ```bash
   make build
   # or
   go build -o porter ./cmd/server
   ```

4. **Run tests**:
   ```bash
   make test
   # or
   go test ./...
   ```

### Development Workflow

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Follow the coding standards below
   - Add tests for new functionality
   - Update documentation as needed

3. **Test your changes**:
   ```bash
   make test
   make build
   ./porter --help  # Test CLI
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

5. **Push and create a PR**:
   ```bash
   git push origin feature/your-feature-name
   ```

## Coding Standards

### Go Code Style

- Follow [Effective Go](https://golang.org/doc/effective_go.html)
- Use `gofmt` for code formatting
- Follow the project's existing patterns and conventions
- Use meaningful variable and function names
- Add comments for complex logic

### Project Structure

```
porter/
├── cmd/server/          # Main application entry point
├── pkg/                 # Core packages
│   ├── benchmark/       # Benchmarking functionality
│   ├── cache/          # Caching layer
│   ├── handlers/       # Request handlers
│   ├── infrastructure/ # Infrastructure components
│   ├── models/         # Data models
│   ├── repositories/   # Data access layer
│   ├── services/       # Business logic
│   └── streaming/      # Streaming functionality
├── config/             # Configuration files
├── docs/               # Documentation
├── examples/           # Example code and scripts
└── test/               # Integration tests
```

### Code Organization

- **Packages**: Group related functionality in packages
- **Interfaces**: Define interfaces for testability and flexibility
- **Error Handling**: Use structured error handling with error codes
- **Logging**: Use structured logging with appropriate levels
- **Testing**: Write unit tests for all new functionality

### Testing Guidelines

- **Unit Tests**: Test individual functions and methods
- **Integration Tests**: Test component interactions
- **Benchmark Tests**: Use the built-in benchmarking for performance-critical code
- **Test Coverage**: Aim for high test coverage, especially for new features

Example test structure:
```go
func TestMyFunction(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected string
    }{
        {"normal case", "input", "expected"},
        {"edge case", "", ""},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := MyFunction(tt.input)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

## Benchmarking Contributions

### Adding New TPC-H Queries

To add new TPC-H queries to the benchmarking suite:

1. **Add the query** to `pkg/benchmark/tpch.go`:
   ```go
   "q11": `
       SELECT
           ps_partkey,
           sum(ps_supplycost * ps_availqty) as value
       FROM partsupp, supplier, nation
       WHERE ps_suppkey = s_suppkey
           AND s_nationkey = n_nationkey
           AND n_name = 'GERMANY'
       GROUP BY ps_partkey
       HAVING sum(ps_supplycost * ps_availqty) > (
           SELECT sum(ps_supplycost * ps_availqty) * 0.0001
           FROM partsupp, supplier, nation
           WHERE ps_suppkey = s_suppkey
               AND s_nationkey = n_nationkey
               AND n_name = 'GERMANY'
       )
       ORDER BY value desc`,
   ```

2. **Update the query list** in `GetAllQueries()`:
   ```go
   func GetAllQueries() []string {
       return []string{"q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11"}
   }
   ```

3. **Test the new query**:
   ```bash
   ./porter bench --query q11 --scale 0.01
   ```

### Adding New Benchmark Suites

To add support for other benchmark suites (e.g., TPC-DS):

1. **Create a new package** `pkg/benchmark/tpcds.go`
2. **Implement the benchmark interface**:
   ```go
   type TPCDSRunner struct {
       // Implementation
   }
   
   func (r *TPCDSRunner) Run(ctx context.Context, config BenchmarkConfig) (*BenchmarkResult, error) {
       // Implementation
   }
   ```

3. **Add CLI support** in `cmd/server/main.go`
4. **Update documentation** in `docs/benchmarking.md`

### Benchmark Testing

When contributing to benchmarking features:

1. **Test with multiple scale factors**:
   ```bash
   ./porter bench --query q1 --scale 0.01
   ./porter bench --query q1 --scale 0.1
   ./porter bench --query q1 --scale 1
   ```

2. **Test all output formats**:
   ```bash
   ./porter bench --query q1 --format table
   ./porter bench --query q1 --format json
   ./porter bench --query q1 --format arrow
   ```

3. **Test error conditions**:
   - Invalid query names
   - Invalid scale factors
   - Timeout conditions
   - Database connection issues

## Pull Request Guidelines

### Before Submitting

1. **Ensure tests pass**:
   ```bash
   make test
   make build
   ```

2. **Run benchmarks** (if applicable):
   ```bash
   ./porter bench --all --scale 0.01
   ```

3. **Update documentation**:
   - README.md for user-facing changes
   - docs/ for detailed documentation
   - Code comments for complex logic

4. **Check formatting**:
   ```bash
   go fmt ./...
   go vet ./...
   ```

### PR Description Template

```markdown
## Description
Brief description of the changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing completed
- [ ] Benchmarks run (if applicable)

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests added/updated
- [ ] No breaking changes (or breaking changes documented)
```

### Review Process

1. **Automated Checks**: CI/CD pipeline runs tests and checks
2. **Code Review**: At least one maintainer reviews the PR
3. **Testing**: Changes are tested in various environments
4. **Documentation**: Documentation is reviewed and updated
5. **Merge**: PR is merged after approval

## Release Process

### Versioning

Porter follows [Semantic Versioning](https://semver.org/):
- **Major**: Breaking changes
- **Minor**: New features (backward compatible)
- **Patch**: Bug fixes (backward compatible)

### Release Checklist

- [ ] All tests passing
- [ ] Documentation updated
- [ ] Benchmarks run and documented
- [ ] CHANGELOG updated
- [ ] Version tags created
- [ ] Release notes written

## Getting Help

### Questions and Discussions

- **GitHub Discussions**: For questions, ideas, and general discussion
- **Issues**: For bug reports and feature requests
- **Pull Requests**: For code contributions

### Mentorship

New contributors are welcome! If you're new to the project:

1. **Start small**: Look for issues labeled "good first issue"
2. **Ask questions**: Don't hesitate to ask for clarification
3. **Join discussions**: Participate in community discussions
4. **Be patient**: Learning takes time, and that's okay

### Resources

- [Go Documentation](https://golang.org/doc/)
- [Apache Arrow Documentation](https://arrow.apache.org/docs/)
- [Flight SQL Specification](https://arrow.apache.org/docs/format/FlightSql.html)
- [DuckDB Documentation](https://duckdb.org/docs/)

## Recognition

Contributors are recognized in several ways:

- **Contributors list**: GitHub automatically tracks contributions
- **Release notes**: Significant contributions are mentioned
- **Documentation**: Contributors are credited in relevant docs
- **Community**: Active contributors are invited to join maintainer discussions

Thank you for contributing to Porter! Your contributions help make analytics faster, simpler, and more accessible to everyone. 