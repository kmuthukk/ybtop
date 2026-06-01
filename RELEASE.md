# ybtop — Release Guide (GitHub Releases, no PyPI)

How to build `ybtop` as a Python wheel and distribute it via **GitHub Releases** — no PyPI account needed. Users install directly from the release URL with a single `pip` command.

---

## Prerequisites (one-time)

```bash
pip install --upgrade build
```

Install the `gh` CLI if not already present: <https://cli.github.com/>

```bash
gh auth login   # authenticate once
```

---

## Step 1 — Bump the version

Edit **both** files — they must match:

**`pyproject.toml`** (line 3):
```toml
version = "0.1.11"
```

**`src/ybtop/__init__.py`** (line 1):
```python
__version__ = "0.1.11"
```

Add an entry to `CHANGELOG.md`:
```markdown
## [0.1.11] — YYYY-MM-DD

### Added / Changed / Fixed
- ...
```

Commit and push:
```bash
git add pyproject.toml src/ybtop/__init__.py CHANGELOG.md
git commit -m "chore: bump version to 0.1.11"
git push
```

---

## Step 2 — Build the wheel

Run from the repo root (where `pyproject.toml` lives):

```bash
# Clean previous build artifacts
rm -rf dist/ build/ src/ybtop.egg-info/

# Build wheel + sdist
python -m build
```

This produces two files in `dist/`:

```
dist/
  ybtop-0.1.11-py3-none-any.whl   ← wheel  (users install this)
  ybtop-0.1.11.tar.gz             ← source dist (attach for completeness)
```

### What the wheel filename means

| Segment | Value | Meaning |
|---|---|---|
| Python tag | `py3` | Python 3.x |
| ABI tag | `none` | No C extensions — pure Python |
| Platform tag | `any` | macOS, Linux, Windows |

A wheel built on macOS installs identically on Linux and Windows because `ybtop` has no compiled extensions. The only platform-specific dependency (`psycopg[binary]`) is resolved by pip on the user's machine at install time.

---

## Step 3 — Verify locally before releasing

```bash
# Create a clean venv
python -m venv /tmp/ybtop-test
source /tmp/ybtop-test/bin/activate     # Windows: /tmp/ybtop-test/Scripts/activate

# Install from the wheel
pip install dist/ybtop-0.1.11-py3-none-any.whl

# Smoke test
ybtop --help
python -c "import ybtop; print(ybtop.__version__)"   # should print 0.1.11

deactivate
```

---

## Step 4 — Tag and publish the GitHub Release

```bash
git tag v0.1.11
git push origin v0.1.11
```

Create the release and attach both dist files:

```bash
gh release create v0.1.11 \
  dist/ybtop-0.1.11-py3-none-any.whl \
  dist/ybtop-0.1.11.tar.gz \
  --title "v0.1.11" \
  --notes "$(sed -n '/## \[0.1.11\]/,/## \[0.1/p' CHANGELOG.md | head -n -1)"
```

The release will be live at:
```
https://github.com/kmuthukk/ybtop/releases/tag/v0.1.11
```

---

## How users install it

### Option A — Direct pip install from the release URL (recommended)

No download needed. pip fetches and installs the wheel in one command:

```bash
pip install https://github.com/kmuthukk/ybtop/releases/download/v0.1.11/ybtop-0.1.11-py3-none-any.whl
```

### Option B — Download the wheel and install locally

1. Go to the Releases page and download `ybtop-0.1.11-py3-none-any.whl`
2. Run:
```bash
pip install ybtop-0.1.11-py3-none-any.whl
```

### Option C — Install latest from source (no wheel needed)

Installs directly from the repo's default branch — always the latest code:

```bash
pip install git+https://github.com/kmuthukk/ybtop.git
```

Pin to a specific tag:

```bash
pip install git+https://github.com/kmuthukk/ybtop.git@v0.1.11
```

> All three options automatically install `psycopg[binary]` and `rich` as dependencies.

---

## Upgrading to a newer release

```bash
# Direct URL install (replace version number)
pip install --upgrade https://github.com/kmuthukk/ybtop/releases/download/v0.1.12/ybtop-0.1.12-py3-none-any.whl

# Or from git
pip install --upgrade git+https://github.com/kmuthukk/ybtop.git@v0.1.12
```

---

## Cross-platform compatibility

The repo is already correctly configured — no platform-specific build matrix needed:

| Item | Location | Status |
|---|---|---|
| `Operating System :: OS Independent` classifier | `pyproject.toml` | ✅ Set |
| No C extensions → `py3-none-any` wheel | `src/ybtop/` | ✅ Pure Python |
| `psycopg[binary]` resolved per-platform by pip | `pyproject.toml` | ✅ Automatic |
| `requires-python = ">=3.9"` | `pyproject.toml` | ✅ Set |
| Web assets bundled via `package-data` | `pyproject.toml` | ✅ Set |

---

## Release checklist

```
[ ] Bump version in pyproject.toml and src/ybtop/__init__.py
[ ] Add CHANGELOG.md entry
[ ] git commit + push
[ ] rm -rf dist/ build/ src/ybtop.egg-info/
[ ] python -m build
[ ] Install wheel locally and smoke test  (ybtop --help)
[ ] git tag v0.x.x && git push origin v0.x.x
[ ] gh release create v0.x.x dist/ybtop-*.whl dist/ybtop-*.tar.gz ...
[ ] Share the install URL with users
```

---

## Optional: Also publish to PyPI

If you want users to be able to `pip install ybtop` without a URL, publish to PyPI in addition to the GitHub Release. The dist files from Step 2 are reused — no rebuild needed.

### Additional prerequisite

```bash
pip install --upgrade twine
```

### Set up PyPI credentials (one-time)

1. Register at <https://pypi.org/account/register/>
2. Enable 2FA (required for project owners)
3. Go to **Account Settings → API tokens → Add API token**
4. Set scope to **Entire account** for the first upload; narrow to per-project after
5. Copy the token (starts with `pypi-`)

Save it to `~/.pypirc`:

```ini
[distutils]
index-servers = pypi

[pypi]
  username = __token__
  password = pypi-YOUR_TOKEN_HERE
```

Or export for a single session:

```bash
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=pypi-YOUR_TOKEN_HERE
```

### Validate metadata before uploading

```bash
twine check dist/*
# Every item should show: PASSED
```

### Test on TestPyPI first (recommended)

Upload to the test registry to catch issues without touching the real index:

```bash
twine upload --repository testpypi dist/*
```

Verify the install from TestPyPI:

```bash
pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  ybtop==0.1.11
```

> `--extra-index-url` points to real PyPI so that `psycopg` and `rich` resolve correctly.

### Publish to PyPI

```bash
twine upload dist/*
```

The package is live at `https://pypi.org/project/ybtop/`. Users can now install without a URL:

```bash
pip install ybtop
pip install ybtop==0.1.11   # pin a specific version
```

### PyPI release checklist (addendum)

```
[ ] pip install --upgrade twine
[ ] twine check dist/*
[ ] twine upload --repository testpypi dist/*   (optional but recommended)
[ ] Verify install from TestPyPI
[ ] twine upload dist/*
[ ] Confirm live at https://pypi.org/project/ybtop/
```
