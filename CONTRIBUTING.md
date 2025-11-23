# Contributing to MEDS ETL

Thank you for contributing to MEDS ETL! This guide will help you set up your development environment and understand our workflow.

## 🚀 Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/Medical-Event-Data-Standard/meds_etl.git
cd meds_etl

# Install with development dependencies
pip install uv  # If you don't have uv
uv pip install -e ".[dev]"
```

### 2. Set Up Pre-commit Hooks (Recommended)

We use `pre-commit` to automatically format code before commits:

```bash
# Install pre-commit hooks
pre-commit install

# Now black, isort, and ruff will run automatically on every commit!
```

**What happens on commit:**
- 🎨 **black** - Automatically formats your code
- 🔤 **isort** - Sorts and organizes imports
- 🔍 **ruff** - Checks for code quality issues and auto-fixes when possible

If any hook fails, the commit is blocked and you can review the changes.

### 3. Manual Formatting (Alternative)

If you prefer to format manually before committing:

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Check for linting issues
ruff check src/ tests/

# Run tests
pytest tests/ -v
```

## 🧪 Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_omop_streaming.py -v

# Run with coverage
pytest tests/ --cov=meds_etl --cov-report=html
```

## 📝 Code Style

We follow these style guidelines:

- **Line length:** 120 characters
- **Python version:** 3.10+
- **Formatting:** black (automatic)
- **Import sorting:** isort with black profile (automatic)
- **Linting:** ruff

All configurations are in `pyproject.toml`.

## 🔄 Development Workflow

1. **Create a branch:**
   ```bash
   git checkout -b feature/my-new-feature
   ```

2. **Make your changes:**
   - Add tests for new functionality
   - Update documentation if needed
   - Ensure tests pass: `pytest tests/ -v`

3. **Commit your changes:**
   ```bash
   git add .
   git commit -m "feat: add new feature"
   # Pre-commit hooks will run automatically!
   ```

4. **Push and create a PR:**
   ```bash
   git push origin feature/my-new-feature
   ```

## 🎯 Pull Request Guidelines

- **Tests:** All tests must pass (30+ tests in the suite)
- **Coverage:** Maintain or improve test coverage
- **Formatting:** Code must pass black, isort, and ruff checks
- **Documentation:** Update README/docstrings for new features
- **Commit messages:** Use clear, descriptive commit messages

## 🐛 Reporting Issues

When reporting issues, please include:

- Python version
- Operating system
- Steps to reproduce
- Expected vs. actual behavior
- Relevant error messages/logs

## 💡 Questions?

- **Issues:** [GitHub Issues](https://github.com/Medical-Event-Data-Standard/meds_etl/issues)
- **Discussions:** [GitHub Discussions](https://github.com/Medical-Event-Data-Standard/meds_etl/discussions)

## 📦 Optional Dependencies

```bash
# C++ backend (for faster sorting)
pip install meds_etl[cpp]

# Benchmarking tools
pip install meds_etl[benchmarking]

# All dev tools
pip install meds_etl[dev]
```

---

Happy coding! 🎉

