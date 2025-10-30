# Quick Start Guide

## Clone and Setup

```bash
# Clone to your desired directory name
git clone https://github.com/kchauhan_mcafee/databricks-eda-template.git my-eda-project
cd my-eda-project

# Disconnect from template repo and start fresh
rm -rf .git
git init
git add -A
git commit -m "Initial commit from databricks-eda-template"

# Setup environment
cp .env.template .env
# Edit .env with your Databricks credentials:
# - DATABRICKS_ACCESS_TOKEN
# - DATABRICKS_SERVER_HOSTNAME  
# - DATABRICKS_HTTP_PATH

# Install dependencies (choose one)
# Option 1: Using uv (recommended)
uv venv && source .venv/bin/activate && uv sync

# Option 2: Using pip
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

# Configure Databricks CLI (if needed)
export DATABRICKS_CLI_DO_NOT_EXECUTE_NEWER_VERSION=1
./.venv/bin/databricks configure --oauth --host ${DATABRICKS_SERVER_HOSTNAME}

# Refresh token and test connection
echo "sql" | python3 utils/token_auth_setup.py --refresh-token
python3 utils/token_auth_setup.py --test-connection
```

## Start Volleying

```
You: "Let's volley on [dataset] to understand [question]"

Claude: [writes notebooks/temp_code/01-analysis.py, queries Databricks, shows results]

You: "Dig deeper into [specific finding]"

Claude: [iterates with more analysis]

You: "Punch it!"

Claude: [creates notebooks/01-analysis.ipynb with all code + docs]
```

## What You Get

- **`utils/databricks_query.py`** - Secure query client (SELECT, SHOW, DESCRIBE, WITH)
- **`utils/token_auth_setup.py`** - Token management
- **Volleying workflow** - Iterative EDA with Claude
- **Auto QA** - Jupytext validation built-in

That's it. See [README.md](README.md) for more details.