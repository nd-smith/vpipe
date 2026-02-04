# Simulation Mode Removal - Implementation Plan

**Created:** 2026-02-03
**Status:** Planning
**Estimated Effort:** 8-12 hours
**Risk Level:** Medium-High

## Executive Summary

This plan outlines the complete removal of simulation mode from the vpipe project. Simulation mode was a local testing framework that allowed developers to run the pipeline without cloud infrastructure. It has been superseded by other testing approaches and adds unnecessary complexity.

**Key Statistics:**
- ~20 files to delete entirely
- ~8 files to modify
- ~2,000-2,500 lines of code to remove
- Touches core worker initialization paths

---

## Table of Contents

1. [Pre-Implementation Checklist](#pre-implementation-checklist)
2. [Phase 1: Remove Test Infrastructure](#phase-1-remove-test-infrastructure)
3. [Phase 2: Remove Core Simulation Module](#phase-2-remove-core-simulation-module)
4. [Phase 3: Update Worker Code](#phase-3-update-worker-code)
5. [Phase 4: Update Entry Point](#phase-4-update-entry-point)
6. [Phase 5: Final Cleanup](#phase-5-final-cleanup)
7. [Phase 6: Validation](#phase-6-validation)
8. [Rollback Strategy](#rollback-strategy)
9. [Risk Mitigation](#risk-mitigation)

---

## Pre-Implementation Checklist

**Before starting, ensure:**

- [ ] Create a feature branch: `git checkout -b remove-simulation-mode`
- [ ] Confirm no active development depends on simulation mode
- [ ] Review recent commits for simulation-related changes
- [ ] Backup local `/tmp/pcesdopodappv1_simulation/` data (if needed)
- [ ] Document alternative testing strategy for future use
- [ ] Run full test suite to establish baseline: `pytest`
- [ ] Verify all workers start correctly in current state

**Verification Commands:**
```bash
# Check for any uncommitted work
git status

# Run test suite
pytest -v

# Check for simulation mode usage in running processes
ps aux | grep simulation
```

---

## Phase 1: Remove Test Infrastructure

**Risk Level:** Low
**Estimated Time:** 30 minutes
**Rollback:** Git revert

### 1.1 Delete Test Files

```bash
# Remove simulation-specific tests
rm -rf tests/pipeline/simulation/
rm -f tests/test_url_validation_simulation.py
```

**Files to delete:**
- `tests/pipeline/simulation/test_storage.py`
- `tests/pipeline/simulation/test_storage_integration.py`
- `tests/test_url_validation_simulation.py`
- `tests/pipeline/simulation/__init__.py` (if exists)

### 1.2 Delete Scripts

```bash
# Remove simulation scripts
rm -f scripts/test_delta_simulation.py
rm -f scripts/verify_simulation.py
rm -f scripts/inspect_simulation_delta.py
rm -f scripts/start_simulation.sh
rm -f scripts/stop_simulation.sh
rm -f scripts/cleanup_simulation_data.sh
rm -f scripts/monitor_simulation.sh
rm -f scripts/docker/docker-compose.simulation.yml
```

### 1.3 Validation

```bash
# Verify files are deleted
git status | grep deleted

# Run remaining tests
pytest -v
```

**Expected Outcome:**
- All simulation test files removed
- All simulation scripts removed
- Remaining test suite passes (no dependencies on deleted tests)

**Commit Point:**
```bash
git add -A
git commit -m "Remove simulation test infrastructure and scripts"
```

---

## Phase 2: Remove Core Simulation Module

**Risk Level:** Low
**Estimated Time:** 15 minutes
**Rollback:** Git revert

### 2.1 Delete Simulation Module Directory

```bash
# Remove entire simulation module
rm -rf src/pipeline/simulation/
```

**Files removed:**
- `src/pipeline/simulation/__init__.py` (57 lines)
- `src/pipeline/simulation/config.py` (458 lines)
- `src/pipeline/simulation/storage.py` (511 lines)
- `src/pipeline/simulation/claimx_api_mock.py` (150+ lines)
- `src/pipeline/simulation/factories.py` (318 lines)
- `src/pipeline/simulation/dummy_producer.py` (250+ lines)

### 2.2 Delete Configuration Files

```bash
# Remove simulation configuration
rm -f config/simulation.yaml
rm -f .env.simulation
rm -f docs/.env.simulation.example
```

### 2.3 Validation

```bash
# Verify module is completely removed
ls -la src/pipeline/ | grep simulation

# Check for broken imports (will fail, expected at this stage)
python -c "from pipeline.simulation import SimulationConfig" 2>&1 | grep "No module named"
```

**Expected Outcome:**
- Simulation module directory completely removed
- Configuration files deleted
- Import attempts fail (expected until we clean up references)

**Commit Point:**
```bash
git add -A
git commit -m "Remove core simulation module and configuration files"
```

---

## Phase 3: Update Worker Code

**Risk Level:** Medium-High
**Estimated Time:** 2-3 hours
**Rollback:** Git revert individual commits

### 3.1 Update Pipeline Configuration

**File:** `src/config/pipeline_config.py`

**Changes:**
1. Remove `simulation` field from `PipelineConfig` dataclass (around line 901-910)
2. Remove simulation-related imports at top of file

**Before:**
```python
@dataclass
class PipelineConfig:
    # ... other fields ...
    simulation: Optional[SimulationConfig] = None  # REMOVE THIS
```

**After:**
```python
@dataclass
class PipelineConfig:
    # ... other fields ...
    # simulation field removed
```

**Search for imports:**
```bash
grep -n "from.*simulation" src/config/pipeline_config.py
grep -n "import.*simulation" src/config/pipeline_config.py
```

**Validation:**
```bash
# Test configuration loads without simulation field
python -c "from config.pipeline_config import PipelineConfig; print('OK')"
```

**Commit Point:**
```bash
git add src/config/pipeline_config.py
git commit -m "Remove simulation field from PipelineConfig"
```

### 3.2 Update ClaimX Download Worker

**File:** `src/pipeline/claimx/workers/download_worker.py`

**Changes:**
1. Remove `simulation_config` parameter from `__init__` (line 114)
2. Remove `_simulation_config` storage (lines 180-187)
3. Remove localhost URL logic if tied to simulation

**Current signature (lines 108-114):**
```python
def __init__(
    self,
    config: KafkaConfig,
    domain: str = "claimx",
    temp_dir: Path | None = None,
    instance_id: str | None = None,
    simulation_config: Any | None = None,  # REMOVE
):
```

**Updated signature:**
```python
def __init__(
    self,
    config: KafkaConfig,
    domain: str = "claimx",
    temp_dir: Path | None = None,
    instance_id: str | None = None,
):
```

**Remove lines 180-187:**
```python
# DELETE THIS BLOCK
self._simulation_config = simulation_config
if simulation_config is not None:
    logger.info("Simulation mode enabled - localhost URLs will be allowed")
```

**Search for other references:**
```bash
grep -n "simulation_config" src/pipeline/claimx/workers/download_worker.py
grep -n "_simulation_config" src/pipeline/claimx/workers/download_worker.py
```

**Validation:**
```bash
# Test worker imports
python -c "from pipeline.claimx.workers.download_worker import ClaimXDownloadWorker; print('OK')"
```

**Commit Point:**
```bash
git add src/pipeline/claimx/workers/download_worker.py
git commit -m "Remove simulation_config from ClaimX download worker"
```

### 3.3 Update ClaimX Delta Writer

**File:** `src/pipeline/claimx/writers/delta_entities.py`

**Changes:**
1. Remove simulation mode checks (lines 283-288, 419)
2. Remove local delta mode logic
3. Remove simulation imports

**Search for simulation references:**
```bash
grep -n "is_simulation_mode" src/pipeline/claimx/writers/delta_entities.py
grep -n "simulation" src/pipeline/claimx/writers/delta_entities.py
```

**Example removal (around lines 283-288):**
```python
# DELETE THIS BLOCK
if is_simulation_mode():
    simulation_config = get_simulation_config()
    self.use_local_delta = True
```

**Keep only production behavior:**
```python
# Production Delta Lake only
self.use_local_delta = False
```

**Validation:**
```bash
# Test writer imports
python -c "from pipeline.claimx.writers.delta_entities import ClaimXEntityDeltaWriter; print('OK')"
```

**Commit Point:**
```bash
git add src/pipeline/claimx/writers/delta_entities.py
git commit -m "Remove simulation mode from ClaimX delta writer"
```

### 3.4 Update ClaimX Worker Runners

**File:** `src/pipeline/runners/claimx_runners.py`

**Critical Changes Required:**

#### A. `run_claimx_enrichment_worker()` (lines 106-157)

**Remove:**
- `simulation_mode` parameter
- `simulation_config` parameter
- Conditional logic for `create_simulation_enrichment_worker()`
- Keep only production worker initialization

**Before:**
```python
async def run_claimx_enrichment_worker(
    config: KafkaConfig,
    instance_id: str | None = None,
    simulation_mode: bool = False,  # REMOVE
    simulation_config: Any | None = None,  # REMOVE
) -> None:
    if simulation_mode:
        # REMOVE THIS ENTIRE BLOCK
        worker = create_simulation_enrichment_worker(
            config=config,
            domain="claimx",
            instance_id=instance_id,
            simulation_config=simulation_config,
        )
    else:
        # KEEP THIS - becomes the only path
        api_client = ClaimXApiClient()
        worker = ClaimXEnrichmentWorker(...)
```

**After:**
```python
async def run_claimx_enrichment_worker(
    config: KafkaConfig,
    instance_id: str | None = None,
) -> None:
    # Only production path
    api_client = ClaimXApiClient()
    worker = ClaimXEnrichmentWorker(
        config=config,
        api_client=api_client,
        domain="claimx",
        instance_id=instance_id,
    )
    await worker.run()
```

#### B. `run_claimx_download_worker()` (lines 160-196)

**Remove:**
- `simulation_config` parameter passing to worker

**Before:**
```python
worker = ClaimXDownloadWorker(
    config=config,
    domain="claimx",
    temp_dir=temp_dir,
    instance_id=instance_id,
    simulation_config=simulation_config,  # REMOVE
)
```

**After:**
```python
worker = ClaimXDownloadWorker(
    config=config,
    domain="claimx",
    temp_dir=temp_dir,
    instance_id=instance_id,
)
```

#### C. `run_claimx_upload_worker()` (lines 199-242)

**Remove:**
- `simulation_mode` parameter
- `simulation_config` parameter
- Conditional logic for `create_simulation_upload_worker()`

**Before:**
```python
async def run_claimx_upload_worker(
    config: KafkaConfig,
    onelake_client: OneLakeClient,
    instance_id: str | None = None,
    simulation_mode: bool = False,  # REMOVE
    simulation_config: Any | None = None,  # REMOVE
) -> None:
    if simulation_mode:
        # REMOVE THIS ENTIRE BLOCK
        worker = create_simulation_upload_worker(...)
    else:
        # KEEP THIS
        worker = ClaimXUploadWorker(...)
```

**After:**
```python
async def run_claimx_upload_worker(
    config: KafkaConfig,
    onelake_client: OneLakeClient,
    instance_id: str | None = None,
) -> None:
    # Only production path
    worker = ClaimXUploadWorker(
        config=config,
        onelake_client=onelake_client,
        domain="claimx",
        instance_id=instance_id,
    )
    await worker.run()
```

**Remove simulation imports at top of file:**
```bash
grep -n "from pipeline.simulation" src/pipeline/runners/claimx_runners.py
```

**Validation:**
```bash
# Test runner imports
python -c "from pipeline.runners.claimx_runners import run_claimx_enrichment_worker; print('OK')"
```

**Commit Point:**
```bash
git add src/pipeline/runners/claimx_runners.py
git commit -m "Remove simulation mode from ClaimX worker runners"
```

### 3.5 Update XACT Worker Runners

**File:** `src/pipeline/runners/verisk_runners.py`

**Functions to update:**

#### A. `run_xact_enrichment_worker()` (lines 202-236)

Remove `simulation_mode` and `simulation_config` parameters if present.

#### B. `run_upload_worker()` (lines 279-312)

Remove simulation-related parameters and conditional logic.

**Search for all references:**
```bash
grep -n "simulation" src/pipeline/runners/verisk_runners.py
```

**Validation:**
```bash
# Test runner imports
python -c "from pipeline.runners.verisk_runners import run_xact_enrichment_worker; print('OK')"
```

**Commit Point:**
```bash
git add src/pipeline/runners/verisk_runners.py
git commit -m "Remove simulation mode from XACT worker runners"
```

### 3.6 Update Worker Registry

**File:** `src/pipeline/runners/registry.py` (lines 101-151)

**Changes:**
1. Remove `simulation_mode` parameter from all registry functions
2. Remove simulation config passing to worker runners
3. Update all runner function calls to remove simulation parameters

**Search for references:**
```bash
grep -n "simulation" src/pipeline/runners/registry.py
```

**Example change:**
```python
# Before
await run_claimx_enrichment_worker(
    config=kafka_config,
    instance_id=instance_id,
    simulation_mode=simulation_mode,  # REMOVE
    simulation_config=simulation_config,  # REMOVE
)

# After
await run_claimx_enrichment_worker(
    config=kafka_config,
    instance_id=instance_id,
)
```

**Validation:**
```bash
# Test registry imports
python -c "from pipeline.runners.registry import get_worker_runner; print('OK')"
```

**Commit Point:**
```bash
git add src/pipeline/runners/registry.py
git commit -m "Remove simulation mode from worker registry"
```

---

## Phase 4: Update Entry Point

**Risk Level:** High
**Estimated Time:** 1 hour
**Rollback:** Git revert

### 4.1 Update Main Entry Point

**File:** `src/pipeline/__main__.py`

**Changes Required:**

#### A. Remove CLI Argument (line 253)

**Search for:**
```bash
grep -n "\-\-simulation-mode" src/pipeline/__main__.py
```

**Remove argument definition:**
```python
# DELETE THIS
parser.add_argument(
    "--simulation-mode",
    action="store_true",
    help="Enable simulation mode for local testing",
)
```

#### B. Remove Simulation Detection (lines 562-568)

**Search for:**
```bash
grep -n "SIMULATION_MODE" src/pipeline/__main__.py
```

**Remove:**
```python
# DELETE THIS BLOCK
simulation_mode = args.simulation_mode or os.getenv("SIMULATION_MODE", "").lower() == "true"
```

#### C. Remove Simulation Initialization (lines 572-598)

**Remove entire initialization block:**
```python
# DELETE THIS ENTIRE BLOCK
if simulation_mode:
    logger.info("Simulation mode enabled")
    sim_config = SimulationConfig.from_env(enabled=True)
    set_simulation_config(sim_config)
    sim_config.ensure_directories()
    logger.info(f"Simulation storage path: {sim_config.local_storage_path}")
```

#### D. Remove Simulation Parameter from Registry Calls (lines 630, 642)

**Search for:**
```bash
grep -n "simulation_mode=" src/pipeline/__main__.py
```

**Before:**
```python
await run_worker(
    worker_type=args.worker_type,
    config=kafka_config,
    simulation_mode=simulation_mode,  # REMOVE
)
```

**After:**
```python
await run_worker(
    worker_type=args.worker_type,
    config=kafka_config,
)
```

#### E. Remove Simulation Imports

**Search for:**
```bash
grep -n "from.*simulation" src/pipeline/__main__.py
grep -n "import.*simulation" src/pipeline/__main__.py
```

**Remove lines like:**
```python
from pipeline.simulation import SimulationConfig, set_simulation_config
```

**Validation:**
```bash
# Test that main module loads
python -m pipeline --help

# Verify --simulation-mode flag is gone
python -m pipeline --help | grep simulation
```

**Commit Point:**
```bash
git add src/pipeline/__main__.py
git commit -m "Remove simulation mode from main entry point"
```

---

## Phase 5: Final Cleanup

**Risk Level:** Low
**Estimated Time:** 30 minutes
**Rollback:** Git revert

### 5.1 Search for Remaining References

**Comprehensive search:**
```bash
# Search all Python files for simulation references
grep -r "simulation" --include="*.py" src/ | grep -v "__pycache__"

# Search configuration files
grep -r "simulation" --include="*.yaml" config/
grep -r "simulation" --include="*.yml" .
grep -r "SIMULATION" --include=".env*" .

# Search documentation
grep -r "simulation" --include="*.md" docs/
grep -r "simulation" README.md
```

### 5.2 Update Documentation

**Files to check and update:**
- `README.md` - Remove simulation mode references
- `docs/` - Remove simulation mode documentation
- `CHANGELOG.md` - Add entry about removal

**Documentation changes:**
```bash
# Search for simulation in docs
find docs/ -name "*.md" -exec grep -l "simulation" {} \;
```

### 5.3 Clean Up Import Statements

**Search for orphaned imports:**
```bash
# Find any remaining simulation imports
grep -rn "from pipeline.simulation" src/
grep -rn "import.*simulation" src/
```

### 5.4 Update .gitignore (if needed)

**Check if simulation artifacts are in .gitignore:**
```bash
grep -n "simulation" .gitignore
```

**Remove lines like:**
```
# Simulation mode
/tmp/pcesdopodappv1_simulation/
.env.simulation
```

**Commit Point:**
```bash
git add -A
git commit -m "Final cleanup: remove simulation references from docs and configs"
```

---

## Phase 6: Validation

**Risk Level:** N/A
**Estimated Time:** 1-2 hours
**Critical:** Do not skip

### 6.1 Code Quality Checks

```bash
# Check for Python syntax errors
python -m py_compile src/pipeline/**/*.py

# Check imports (if you use isort)
isort --check-only src/

# Check formatting (if you use black)
black --check src/

# Type checking (if you use mypy)
mypy src/
```

### 6.2 Search for Broken Imports

```bash
# Try to import all main modules
python -c "from pipeline import __main__; print('__main__ OK')"
python -c "from pipeline.runners.registry import get_worker_runner; print('registry OK')"
python -c "from pipeline.runners.claimx_runners import run_claimx_enrichment_worker; print('claimx_runners OK')"
python -c "from pipeline.runners.verisk_runners import run_xact_enrichment_worker; print('verisk_runners OK')"
python -c "from config.pipeline_config import PipelineConfig; print('config OK')"
```

### 6.3 Run Test Suite

```bash
# Run full test suite
pytest -v

# Run with coverage
pytest --cov=src --cov-report=term-missing

# Check for skipped tests related to simulation
pytest -v | grep -i simulation
```

### 6.4 Test Worker Startup

**Test each worker type can start:**
```bash
# Test help command works
python -m pipeline --help

# Test worker startup (may fail if Kafka not available, but should not error on imports)
python -m pipeline claimx-enricher --help 2>&1 | grep -v "Kafka"
python -m pipeline claimx-download --help 2>&1 | grep -v "Kafka"
python -m pipeline claimx-upload --help 2>&1 | grep -v "Kafka"
python -m pipeline xact-enricher --help 2>&1 | grep -v "Kafka"
```

### 6.5 Verify No Simulation Artifacts Remain

```bash
# Final comprehensive search
echo "=== Python files ==="
grep -r "simulation" --include="*.py" src/ | wc -l

echo "=== Config files ==="
grep -r "simulation" --include="*.yaml" config/ | wc -l

echo "=== Env files ==="
grep -r "SIMULATION" --include=".env*" . | wc -l

echo "=== Should all be 0 ==="
```

### 6.6 Manual Review Checklist

- [ ] All tests pass
- [ ] No import errors
- [ ] No simulation references in code
- [ ] Documentation updated
- [ ] Worker help commands work
- [ ] No broken imports in critical paths
- [ ] Git status shows only expected changes

---

## Rollback Strategy

### Full Rollback (Nuclear Option)

```bash
# Abandon all changes
git checkout main
git branch -D remove-simulation-mode
```

### Partial Rollback (Phase-by-Phase)

Each phase has a commit. To rollback a specific phase:

```bash
# List commits in branch
git log --oneline

# Revert specific commit
git revert <commit-hash>

# Or reset to before a commit
git reset --hard <commit-hash>
```

### Emergency Rollback (Production Issue)

If removal causes production issues:

1. **Immediate:** Revert the merge commit
```bash
git revert -m 1 <merge-commit-hash>
git push origin main
```

2. **Create hotfix branch** to restore specific functionality
```bash
git checkout -b hotfix/restore-simulation-partial
# Cherry-pick needed commits from before removal
git cherry-pick <old-commit-hash>
```

---

## Risk Mitigation

### High-Risk Areas

#### 1. Worker Initialization Flow

**Risk:** Workers fail to start due to missing parameters

**Mitigation:**
- Test each worker type individually after changes
- Review worker factory functions carefully
- Keep backup of worker runner signatures

**Rollback trigger:** Any worker fails to initialize

#### 2. Import Dependencies

**Risk:** Circular imports or missing imports

**Mitigation:**
- Test imports before running tests
- Use `python -m py_compile` to catch syntax errors
- Review import statements in modified files

**Rollback trigger:** Import errors in production code paths

#### 3. Configuration Loading

**Risk:** Configuration validation fails without simulation field

**Mitigation:**
- Test config loading independently
- Verify production configs don't reference simulation
- Check default values and optional fields

**Rollback trigger:** Config fails to load in any environment

### Medium-Risk Areas

#### 1. Test Suite Dependencies

**Risk:** Tests fail due to missing simulation fixtures

**Mitigation:**
- Run tests after each phase
- Document any test failures
- Identify tests that need alternative approaches

**Rollback trigger:** More than 10% of tests fail

#### 2. CLI Argument Removal

**Risk:** Scripts or docs reference `--simulation-mode` flag

**Mitigation:**
- Search all scripts for flag usage
- Update any CI/CD pipelines
- Check documentation

**Rollback trigger:** User-facing scripts break

### Low-Risk Areas

#### 1. File Deletions

**Risk:** Accidentally delete production files

**Mitigation:**
- Double-check file paths before `rm -rf`
- Use git to verify deletions
- Review `git status` before committing

**Rollback trigger:** Any production file deleted (immediate rollback)

---

## Post-Implementation

### Documentation Updates Required

- [ ] Update README.md - remove simulation mode section
- [ ] Update developer onboarding docs
- [ ] Update testing strategy docs
- [ ] Add note to CHANGELOG.md
- [ ] Update CI/CD documentation if needed

### Communication

**Notify team about:**
- Simulation mode removal complete
- Alternative testing approaches available
- Any breaking changes to local dev workflow
- Timeline for removal from documentation

### Monitoring

**After merge, monitor for:**
- Worker startup failures
- Configuration errors
- Test failures in CI/CD
- Developer questions about local testing

### Future Improvements

**Consider:**
- Document alternative local testing strategies
- Improve unit test coverage to reduce need for integration tests
- Create lightweight mocking utilities for specific use cases
- Evaluate if any simulation components should be preserved

---

## Success Criteria

✅ **Must Pass:**
- All tests pass (excluding explicitly deleted simulation tests)
- All workers start without errors
- No simulation imports remain in codebase
- No simulation configuration files remain
- Documentation updated

✅ **Nice to Have:**
- Test coverage maintained or improved
- Code complexity reduced
- Fewer dependencies
- Faster test suite execution

---

## Timeline Estimate

| Phase | Estimated Time | Critical Path |
|-------|----------------|---------------|
| Phase 1: Test Infrastructure | 30 min | No |
| Phase 2: Core Module | 15 min | No |
| Phase 3: Worker Code | 2-3 hours | **Yes** |
| Phase 4: Entry Point | 1 hour | **Yes** |
| Phase 5: Cleanup | 30 min | No |
| Phase 6: Validation | 1-2 hours | **Yes** |
| **Total** | **6-8 hours** | |

Add buffer for:
- Unexpected dependencies: +2 hours
- Test fixes: +1-2 hours
- Documentation: +1 hour

**Total with buffer: 10-13 hours**

---

## Notes

- Keep this document updated as implementation progresses
- Mark completed phases with checkboxes
- Document any deviations from plan
- Record any issues encountered and solutions

---

## Appendix: Quick Reference

### Key Files Modified

| File | Lines Changed | Risk |
|------|---------------|------|
| `src/pipeline/__main__.py` | ~40 lines removed | High |
| `src/pipeline/runners/claimx_runners.py` | ~60 lines removed | High |
| `src/pipeline/runners/registry.py` | ~20 lines removed | Medium |
| `src/config/pipeline_config.py` | ~10 lines removed | Low |
| `src/pipeline/claimx/workers/download_worker.py` | ~10 lines removed | Low |
| `src/pipeline/claimx/writers/delta_entities.py` | ~5 lines removed | Low |

### Search Commands

```bash
# Find all simulation references
grep -rn "simulation" src/ --include="*.py"

# Find simulation imports
grep -rn "from.*simulation\|import.*simulation" src/

# Find SIMULATION_MODE env var usage
grep -rn "SIMULATION_MODE" src/

# Find simulation in configs
find config/ -name "*.yaml" -exec grep -H "simulation" {} \;
```

### Test Commands

```bash
# Run all tests
pytest -v

# Run specific test file
pytest tests/path/to/test_file.py -v

# Run with coverage
pytest --cov=src --cov-report=html

# Run only unit tests (skip integration)
pytest -m "not integration" -v
```

---

**End of Implementation Plan**
