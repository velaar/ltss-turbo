# Contributing to LTSS Turbo

First off, thanks for taking the time to contribute! 🎉

## ⚠️ Important Disclaimer

**This is a hobby project created for fun and learning.** There are:
- No guarantees of support or maintenance
- No SLAs or response time commitments  
- No warranties or fitness for any particular purpose
- No obligation to accept contributions or implement features

That said, quality contributions are welcome and appreciated!

## 🤝 Code of Conduct

Be respectful and constructive. This is a hobby project maintained in spare time.

## 🐛 Reporting Bugs

Before creating bug reports, please check existing issues. When creating a bug report, include:

1. **Clear title and description**
2. **Steps to reproduce**
3. **Expected vs actual behavior**
4. **Environment details:**
   - Home Assistant version
   - PostgreSQL version
   - TimescaleDB version (if applicable)
   - Python version
   - Relevant configuration (sanitized)
5. **Logs** (with debug logging enabled)
6. **Diagnostic output** (run `ltss_diagnostics.py`)

## 💡 Suggesting Features

Feature suggestions are welcome! Please:

1. Check if already suggested
2. Provide clear use case
3. Explain why it would benefit others
4. Be patient - this is a hobby project

## 🔧 Pull Requests

### Prerequisites

1. Fork the repository
2. Set up development environment:
   ```bash
   # Clone your fork
   git clone https://github.com/YOUR_USERNAME/ltss-turbo.git
   cd ltss-turbo
   
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements-test.txt
   pip install pre-commit
   pre-commit install
   ```

3. Set up test database:
   ```bash
   docker run -d \
     --name ltss-test-db \
     -e POSTGRES_USER=ltss_test \
     -e POSTGRES_PASSWORD=test \
     -e POSTGRES_DB=test_db \
     -p 5432:5432 \
     timescale/timescaledb-ha:pg14-latest
   ```

### Development Process

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/amazing-feature
   ```

2. **Make your changes:**
   - Follow existing code style
   - Add/update tests
   - Update documentation
   - Add docstrings

3. **Run tests locally:**
   ```bash
   # Format code
   black custom_components/ltss_turbo tests/
   isort custom_components/ltss_turbo tests/
   
   # Run linting
   flake8 custom_components/ltss_turbo tests/
   pylint custom_components/ltss_turbo
   
   # Run tests
   pytest tests/ -v --cov=custom_components/ltss_turbo
   ```

4. **Commit changes:**
   ```bash
   git add .
   git commit -m "Add amazing feature"
   ```
   
   Commit message format:
   - `feat:` New feature
   - `fix:` Bug fix
   - `docs:` Documentation only
   - `test:` Test additions/changes
   - `perf:` Performance improvement
   - `refactor:` Code refactoring
   - `chore:` Maintenance tasks

5. **Push and create PR:**
   ```bash
   git push origin feature/amazing-feature
   ```
   Then create a pull request on GitHub.

### PR Requirements

- [ ] Tests pass (`pytest`)
- [ ] Code is formatted (`black`, `isort`)
- [ ] Linting passes (`flake8`)
- [ ] Coverage maintained or improved (>70%)
- [ ] Documentation updated if needed
- [ ] Commit messages are clear
- [ ] PR description explains changes

### Testing Guidelines

1. **Write tests for new features**
2. **Maintain >70% code coverage**
3. **Test edge cases**
4. **Mock external dependencies**
5. **Use fixtures for reusable test data**

Example test:
```python
def test_numeric_parsing():
    """Test that numeric state parsing works correctly."""
    parser = OptimizedStateParser()
    
    # Test direct numeric
    assert parser.parse_numeric_state("25.5", None, None, "sensor.test", None) == 25.5
    
    # Test percentage
    assert parser.parse_numeric_state("85%", None, None, "sensor.test", None) == 85.0
    
    # Test invalid
    assert parser.parse_numeric_state("invalid", None, None, "sensor.test", None) is None
```

### Performance Guidelines

Since this is a performance-focused integration:

1. **Profile before optimizing** - Use `cProfile` or `py-spy`
2. **Benchmark changes** - Measure impact on:
   - Insert rate (events/second)
   - Memory usage
   - Query performance
3. **Consider caching** - But watch memory usage
4. **Batch operations** - Reduce database round trips
5. **Document performance impacts** in PR

### Documentation

Update documentation for:
- New configuration options
- New features
- Breaking changes
- Grafana query examples
- Performance tips

## 🏗️ Architecture Overview

```
ltss-turbo/
├── custom_components/
│   └── ltss_turbo/
│       ├── __init__.py       # Main integration logic
│       ├── models.py         # SQLAlchemy models & parsing
│       ├── migrations.py     # Database migrations
│       └── manifest.json     # Integration metadata
├── tests/                    # Test suite
├── docs/                     # Documentation
└── scripts/                  # Utility scripts
```

### Key Components

1. **LTSS_DB**: Main threaded processor
2. **BinaryRowEncoder**: Binary COPY protocol encoder
3. **OptimizedStateParser**: State to numeric conversion
4. **ConnectionManager**: Database connection pooling
5. **MetadataManager**: Schema versioning

## 📊 Free Tier CI/CD

Our CI/CD uses GitHub Actions free tier:
- **Public repo**: Unlimited minutes
- **Private repo**: 2000 minutes/month
- **Codecov**: Free for public repos
- **Dependencies**: Minimal to reduce build time

To stay within limits:
- Tests run on push to main/develop and PRs only
- Matrix testing limited to critical Python versions
- Integration tests only for major HA versions
- Caching used extensively

## 🔍 Debugging Tips

1. **Enable debug logging:**
   ```yaml
   logger:
     default: info
     logs:
       custom_components.ltss_turbo: debug
   ```

2. **Check metadata table:**
   ```sql
   SELECT * FROM ltss_turbo_meta ORDER BY key;
   ```

3. **Monitor performance:**
   ```sql
   SELECT 
     COUNT(*) as events_last_hour,
     AVG(state_numeric) as avg_numeric
   FROM ltss_turbo
   WHERE time > NOW() - INTERVAL '1 hour';
   ```

4. **Run diagnostics:**
   ```bash
   python scripts/ltss_diagnostics.py postgresql://user:pass@host/db
   ```

## 🚀 Release Process

Releases are automated when version changes in `manifest.json`:

1. Update version in `manifest.json`
2. Update `CHANGELOG.md`
3. Commit to main branch
4. GitHub Actions creates release automatically

## 💬 Getting Help

- Check [Documentation](README.md)
- Search [existing issues](https://github.com/velaar/ltss-turbo/issues)
- Ask in [Discussions](https://github.com/velaar/ltss-turbo/discussions)

Remember: This is a hobby project - response times vary!

## 🙏 Recognition

Contributors will be acknowledged in:
- Release notes
- README credits section
- Code comments for significant contributions

## 📝 License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

**Thank you for contributing to LTSS Turbo!** 🚀

Even though this is just a hobby project, your contributions help make it better for everyone using it.