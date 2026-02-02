# Simulation Configuration Guide

## Configuration Files

The Pcesdopodappv1 simulation mode uses **dedicated configuration files** separate from production:

```
config/
├── config.yaml          # Production configuration (DO NOT modify for simulation)
├── simulation.yaml      # Simulation configuration (NEW - use this!)
└── README_SIMULATION_CONFIG.md  # This file
```

## Why Separate Config Files?

**Isolation**: Keeps simulation settings completely separate from production
**Safety**: Reduces risk of accidentally using simulation settings in production
**Clarity**: Makes it obvious which config is for what purpose
**Version Control**: Can .gitignore simulation.yaml for local customization

---

## Quick Start

### **1. Using Dedicated Simulation Config (Recommended)**

The `simulation.yaml` file contains all simulation-specific settings:

```bash
# Just set the environment variable
export SIMULATION_MODE=true

# All settings loaded from config/simulation.yaml
python -m pipeline claimx-enricher
```

### **2. Using Environment Variables Only**

Override any setting with environment variables:

```bash
export SIMULATION_MODE=true
export SIMULATION_STORAGE_PATH=/custom/path
export SIMULATION_DELTA_PATH=/custom/delta
export SIMULATION_TRUNCATE_TABLES=true

python -m pipeline claimx-enricher
```

### **3. Using Main Config (Legacy)**

Add `simulation:` section to `config.yaml` (not recommended):

```yaml
# In config/config.yaml (not recommended - use simulation.yaml instead)
simulation:
  enabled: false
  local_storage_path: /tmp/pcesdopodappv1_simulation
  # ... other settings
```

---

## Configuration Priority

Settings are loaded in this order (highest priority first):

1. **Environment Variables** - `SIMULATION_MODE`, `SIMULATION_STORAGE_PATH`, etc.
2. **`config/simulation.yaml`** - Dedicated simulation config (auto-detected)
3. **`config/config.yaml`** - Main config file with `simulation:` section
4. **Defaults** - Hard-coded in `SimulationConfig` class

---

## Configuration Options

### **Core Settings**

```yaml
simulation:
  enabled: true                              # Master switch
  local_storage_path: /tmp/pcesdopodappv1_simulation  # Where uploaded files go
  local_delta_path: /tmp/pcesdopodappv1_simulation/delta  # Where Delta tables go
  allow_localhost_urls: true                 # Allow http://localhost:8765
  fixtures_dir: fixtures                     # Mock API data location
  truncate_tables_on_start: false            # Clean Delta tables on startup
  delta_write_mode: append                   # append | overwrite | merge
```

### **Environment Variable Overrides**

```bash
SIMULATION_MODE=true              # Enable simulation mode
SIMULATION_STORAGE_PATH=/path    # Override storage path
SIMULATION_DELTA_PATH=/path      # Override Delta path
SIMULATION_ALLOW_LOCALHOST=true  # Allow localhost URLs
SIMULATION_FIXTURES_DIR=/path    # Override fixtures location
SIMULATION_TRUNCATE_TABLES=true  # Clean tables on start
SIMULATION_DELTA_WRITE_MODE=append  # Delta write mode
```

---

## Kafka Configuration

The `simulation.yaml` includes Kafka settings for local testing:

```yaml
kafka:
  bootstrap_servers: localhost:9092
  consumer_groups:
    claimx:
      event_ingester: claimx-event-ingester-sim  # Note the -sim suffix
      enrichment: claimx-enricher-sim
      # ... other workers
```

**Why separate consumer groups?**
- Prevents conflict if you run both production and simulation workers
- Allows independent offset tracking
- Makes it clear which workers are in simulation mode

---

## Dummy Data Producer Settings

```yaml
dummy_producer:
  domains: [claimx, xact]
  events_per_minute: 10           # Per domain
  plugin_profile: mixed           # null | "itel_cabinet_api" | "mixed"
  itel_trigger_percentage: 0.3    # 30% of tasks trigger iTel workflow
  max_events: null                # null = unlimited
```

---

## Testing Scenarios

The config includes predefined test scenarios:

```yaml
scenarios:
  quick_test:      # 10 events, ClaimX only
  medium_test:     # 100 events, both domains
  load_test:       # 1000 events, burst mode
  itel_test:       # 20 iTel Cabinet events
```

**Usage:**
```bash
# Run a predefined scenario
SIMULATION_SCENARIO=quick_test ./scripts/run_simulation.sh

# Override scenario settings
SIMULATION_SCENARIO=load_test MAX_EVENTS=5000 ./scripts/run_simulation.sh
```

---

## Customizing Your Config

### **Option 1: Edit simulation.yaml (Local)**

1. Copy the template:
   ```bash
   cp config/simulation.yaml config/simulation.local.yaml
   ```

2. Add to `.gitignore`:
   ```bash
   echo "config/simulation.local.yaml" >> .gitignore
   ```

3. Edit your local copy:
   ```yaml
   # config/simulation.local.yaml
   simulation:
     local_storage_path: /home/myuser/pcesdopodappv1_sim
     truncate_tables_on_start: true  # Your preference
   ```

4. Use it:
   ```bash
   SIMULATION_CONFIG=config/simulation.local.yaml python -m pipeline claimx-enricher
   ```

### **Option 2: Environment Variables Only**

Create a `.env.simulation` file:

```bash
# .env.simulation (add to .gitignore)
export SIMULATION_MODE=true
export SIMULATION_STORAGE_PATH=/home/myuser/pcesdopodappv1_sim
export SIMULATION_TRUNCATE_TABLES=true
export SIMULATION_DELTA_WRITE_MODE=overwrite
```

Load it:
```bash
source .env.simulation
./scripts/run_simulation.sh
```

---

## Verification

Check which config is being used:

```bash
# Start any worker in simulation mode
SIMULATION_MODE=true python -m pipeline claimx-enricher

# Look for log message:
# "Loaded simulation config from: /path/to/config/simulation.yaml"
```

---

## Safety Features

### **Production Protection**

The config includes multiple safety checks:

```yaml
safety:
  require_simulation_mode_env: true   # Must explicitly set SIMULATION_MODE=true
  block_on_production_env: true       # Fail if ENVIRONMENT=production
  warn_on_cloud_config: true          # Warn if cloud settings detected
```

These settings prevent:
- Accidentally running simulation in production
- Using production credentials in simulation
- Mixing production and simulation settings

### **What Happens in Production?**

If `ENVIRONMENT=production` is set:

```python
RuntimeError: SIMULATION MODE CANNOT RUN IN PRODUCTION.
Detected production environment via ENVIRONMENT=production.
Simulation mode is for local development and testing only.
```

**The pipeline will refuse to start.** This is by design.

---

## Migration Guide

### **Moving from Embedded Config to Dedicated Config**

If you have simulation settings in `config.yaml`:

1. **Copy settings** from `config.yaml` `simulation:` section
2. **Paste** into `config/simulation.yaml`
3. **Remove** `simulation:` section from `config.yaml`
4. **Test** with `SIMULATION_MODE=true ./scripts/run_simulation.sh`

### **Moving from Environment Variables to Config File**

If you use many environment variables:

1. **List current variables**:
   ```bash
   env | grep SIMULATION_
   ```

2. **Add to** `config/simulation.yaml`:
   ```yaml
   simulation:
     local_storage_path: /your/custom/path  # Was SIMULATION_STORAGE_PATH
     truncate_tables_on_start: true         # Was SIMULATION_TRUNCATE_TABLES
   ```

3. **Remove** environment variables
4. **Test** with just `SIMULATION_MODE=true`

---

## Best Practices

### **✅ DO**

- Use `config/simulation.yaml` for team-wide simulation settings
- Use environment variables for personal overrides
- Add `simulation.local.yaml` to `.gitignore` for local customization
- Document any non-default settings in team docs
- Use predefined scenarios for common test cases

### **❌ DON'T**

- Don't put simulation settings in `config.yaml` (use dedicated file)
- Don't commit personal paths to version control
- Don't modify production config for simulation testing
- Don't run simulation mode with production environment variables set
- Don't disable safety checks unless you know what you're doing

---

## Troubleshooting

### **"Config file not found"**

```bash
# Check if simulation.yaml exists
ls -la config/simulation.yaml

# If not, create it from production config
cp config/config.yaml config/simulation.yaml
# Then edit simulation.yaml to add simulation: section
```

### **"Using wrong config file"**

```bash
# Force specific config file
SIMULATION_CONFIG=/path/to/config/simulation.yaml python -m pipeline claimx-enricher
```

### **"Settings not taking effect"**

Check priority order:
1. Environment variables override everything
2. Unset conflicting env vars: `unset SIMULATION_STORAGE_PATH`
3. Check config file: `cat config/simulation.yaml`

### **"Production config being used"**

```bash
# Verify SIMULATION_MODE is set
echo $SIMULATION_MODE  # Should print "true"

# Check for ENVIRONMENT variable
echo $ENVIRONMENT  # Should NOT be "production"

# Verify config file location
ls -la config/simulation.yaml
```

---

## Examples

### **Basic Local Testing**

```bash
# Use defaults from simulation.yaml
export SIMULATION_MODE=true
./scripts/run_simulation.sh
```

### **Custom Storage Location**

```bash
export SIMULATION_MODE=true
export SIMULATION_STORAGE_PATH=/mnt/fast-ssd/pcesdopodappv1_sim
export SIMULATION_DELTA_PATH=/mnt/fast-ssd/pcesdopodappv1_sim/delta
./scripts/run_simulation.sh
```

### **Clean Slate Testing**

```bash
export SIMULATION_MODE=true
export SIMULATION_TRUNCATE_TABLES=true  # Clean Delta tables on start
./scripts/cleanup_simulation_data.sh    # Clean files first
./scripts/run_simulation.sh
```

### **iTel Cabinet Testing**

```bash
export SIMULATION_MODE=true
python -m pipeline.simulation.dummy_producer \
  --domains claimx \
  --plugin-profile itel_cabinet_api \
  --max-events 20
```

---

## Summary

**Recommended Setup:**

1. Use `config/simulation.yaml` for all simulation settings
2. Set only `SIMULATION_MODE=true` environment variable
3. Override specific settings with environment variables as needed
4. Create `simulation.local.yaml` for personal customization (add to .gitignore)
5. Never modify `config.yaml` for simulation purposes

This keeps simulation and production completely separate and safe.

---

**Need Help?**

- Read the main simulation guide: `docs/SIMULATION_TESTING.md`
- Check quick reference: `docs/SIMULATION_QUICK_REFERENCE.md`
- Review technical docs: `pipeline/simulation/README.md`
