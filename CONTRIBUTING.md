# Contributing to Crypto Token Intelligence Platform

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Requirements](#testing-requirements)
- [Documentation](#documentation)
- [Pull Request Process](#pull-request-process)
- [Issue Guidelines](#issue-guidelines)

## Code of Conduct

### Our Pledge

We are committed to providing a welcoming and inclusive environment for all contributors.

### Our Standards

- Be respectful and considerate
- Welcome diverse perspectives
- Accept constructive criticism gracefully
- Focus on what's best for the community
- Show empathy towards others

### Unacceptable Behavior

- Harassment or discriminatory language
- Personal attacks or trolling
- Publishing others' private information
- Other conduct inappropriate in a professional setting

## Getting Started

### Prerequisites

1. Read the [Development Guide](docs/DEVELOPMENT.md)
2. Set up your local environment
3. Familiarize yourself with the codebase

### First-Time Setup

```bash
# Fork the repository on GitHub
# Clone your fork
git clone https://github.com/YOUR_USERNAME/token_agents.git
cd token_agents

# Add upstream remote
git remote add upstream https://github.com/ORIGINAL_OWNER/token_agents.git

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r docker/langgraph/requirements.txt
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install
```

### Finding Issues to Work On

- Look for issues labeled `good first issue`
- Check issues labeled `help wanted`
- Review the project roadmap in [CHANGELOG.md](CHANGELOG.md)

## Development Workflow

### 1. Create a Branch

```bash
# Update your fork
git checkout main
git pull upstream main

# Create feature branch
git checkout -b feature/your-feature-name

# Or for bug fixes
git checkout -b fix/bug-description
```

### Branch Naming Convention

- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation changes
- `refactor/` - Code refactoring
- `test/` - Test additions or modifications
- `chore/` - Maintenance tasks

### 2. Make Changes

- Write clean, readable code
- Follow coding standards (see below)
- Add tests for new functionality
- Update documentation as needed
- Keep commits focused and atomic

### 3. Commit Changes

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```bash
# Format: <type>(<scope>): <subject>

git commit -m "feat(agents): add sentiment analysis to news agent"
git commit -m "fix(pipeline): resolve Aurora connection timeout"
git commit -m "docs(api): update endpoint examples"
git commit -m "test(tools): add unit tests for vector search"
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks
- `perf`: Performance improvements

### 4. Push and Create PR

```bash
# Push to your fork
git push origin feature/your-feature-name

# Create Pull Request on GitHub
```

## Coding Standards

### Python Style

Follow PEP 8 with these additions:

```python
# Use type hints
def process_data(items: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process raw items into DataFrame."""
    pass

# Use docstrings (Google style)
def complex_function(param1: str, param2: int) -> bool:
    """One-line summary.
    
    Longer description if needed.
    
    Args:
        param1: Description of param1
        param2: Description of param2
        
    Returns:
        Description of return value
        
    Raises:
        ValueError: When param2 is negative
    """
    pass

# Use descriptive variable names
user_query = "Bitcoin price"  # Good
uq = "Bitcoin price"          # Bad

# Keep functions small (< 50 lines)
# Extract complex logic into helper functions
```

### Code Formatting

```bash
# Format code
black .
isort .

# Check style
flake8 .

# Type checking
mypy agents/ tools/
```

### Configuration Files

**.flake8**:
```ini
[flake8]
max-line-length = 100
exclude = .git,__pycache__,venv,build,dist
ignore = E203,W503
```

**pyproject.toml**:
```toml
[tool.black]
line-length = 100
target-version = ['py311']

[tool.isort]
profile = "black"
line_length = 100
```

## Testing Requirements

### Test Coverage

- Minimum 80% code coverage for new code
- All public functions must have tests
- Critical paths must have integration tests

### Writing Tests

```python
# Unit tests
def test_dedupe_news():
    """Test news deduplication logic."""
    items = [
        {"news_id": "1", "title": "Bitcoin rises"},
        {"news_id": "1", "title": "Bitcoin rises"},  # Duplicate
    ]
    result = dedupe_news_ids(items)
    assert len(result) == 1

# Integration tests
@pytest.mark.integration
async def test_news_pipeline():
    """Test full news processing pipeline."""
    result = await run_once(api_url="...", max_articles=5)
    assert result["iceberg_count"] > 0

# Use fixtures
@pytest.fixture
def sample_data():
    return [{"id": 1, "value": "test"}]

def test_with_fixture(sample_data):
    assert len(sample_data) == 1
```

### Running Tests

```bash
# All tests
pytest

# Specific test file
pytest tests/test_agents.py

# With coverage
pytest --cov=agents --cov-report=html

# Integration tests only
pytest -m integration

# Skip slow tests
pytest -m "not slow"
```

## Documentation

### Code Documentation

- All public functions/classes must have docstrings
- Use Google-style docstrings
- Include examples for complex functions

### User Documentation

When adding features, update:
- README.md (if user-facing)
- Relevant docs/ files
- API.md (if adding endpoints)
- CHANGELOG.md

### Documentation Style

```markdown
# Use clear headings

## Second level

### Third level

# Use code blocks with language
```python
def example():
    pass
```

# Use lists for steps
1. First step
2. Second step
3. Third step

# Use tables for comparisons
| Feature | Option A | Option B |
|---------|----------|----------|
| Speed   | Fast     | Slow     |
```

## Pull Request Process

### Before Submitting

- [ ] Code follows style guidelines
- [ ] All tests pass
- [ ] New tests added for new functionality
- [ ] Documentation updated
- [ ] Commit messages follow convention
- [ ] Branch is up to date with main

### PR Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
Describe testing performed

## Checklist
- [ ] Code follows style guidelines
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] No breaking changes (or documented)

## Related Issues
Closes #123
```

### Review Process

1. Automated checks must pass (CI/CD)
2. At least one maintainer approval required
3. Address review comments
4. Squash commits if requested
5. Maintainer will merge when ready

### After Merge

- Delete your feature branch
- Update your fork
- Close related issues

## Issue Guidelines

### Bug Reports

Use the bug report template:

```markdown
**Describe the bug**
Clear description of the bug

**To Reproduce**
Steps to reproduce:
1. Go to '...'
2. Click on '...'
3. See error

**Expected behavior**
What you expected to happen

**Screenshots**
If applicable

**Environment**
- OS: [e.g., macOS]
- Python version: [e.g., 3.11]
- Version: [e.g., 1.0.0]

**Additional context**
Any other relevant information
```

### Feature Requests

Use the feature request template:

```markdown
**Is your feature request related to a problem?**
Clear description of the problem

**Describe the solution you'd like**
Clear description of desired solution

**Describe alternatives you've considered**
Alternative solutions or features

**Additional context**
Any other relevant information
```

### Questions

For questions:
- Check existing documentation first
- Search existing issues
- Use GitHub Discussions for general questions
- Use Issues for specific technical questions

## Development Tips

### Debugging

```python
# Use logging instead of print
import logging
logger = logging.getLogger(__name__)
logger.debug("Debug message")

# Use debugger
import pdb; pdb.set_trace()

# Or ipdb for better experience
import ipdb; ipdb.set_trace()
```

### Performance

```python
# Profile code
import cProfile
profiler = cProfile.Profile()
profiler.enable()
# ... code to profile ...
profiler.disable()
profiler.print_stats()

# Use timeit for small snippets
import timeit
timeit.timeit('"-".join(str(n) for n in range(100))', number=10000)
```

### Common Pitfalls

1. **Not updating tests**: Always update tests when changing code
2. **Large commits**: Keep commits focused and atomic
3. **Missing documentation**: Document as you code
4. **Ignoring CI failures**: Fix CI issues before requesting review
5. **Not testing locally**: Always test locally before pushing

## Getting Help

- **Documentation**: Check docs/ directory
- **Issues**: Search existing issues
- **Discussions**: Use GitHub Discussions
- **Maintainers**: Tag maintainers in issues/PRs

## Recognition

Contributors will be:
- Listed in CONTRIBUTORS.md
- Mentioned in release notes
- Credited in relevant documentation

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

---

Thank you for contributing! 🎉
