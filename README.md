# Databricks EDA Template with Claude Code

A template for doing Databricks EDA with Claude using the "volleying" workflow.

## Quick Start

```bash
# 1. Clone
git clone <repo-url>
cd claude-databricks-eda-v2

# 2. Setup environment
cp .env.template .env
# Edit .env with your Databricks credentials

# 3. Install dependencies
uv venv && source .venv/bin/activate && uv sync
# Or: python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

# 4. Configure Databricks CLI (if needed)
export DATABRICKS_CLI_DO_NOT_EXECUTE_NEWER_VERSION=1
./.venv/bin/databricks configure --oauth --host https://${DATABRICKS_SERVER_HOSTNAME}

# 5. Refresh token and test
echo "sql" | python3 utils/token_auth_setup.py --refresh-token
python3 utils/token_auth_setup.py --test-connection
```

## The Volleying Workflow

1. **You ask**: "Let's volley on [dataset] to understand [question]"
2. **Claude writes**: Code in `notebooks/temp_code/XX-topic.py`
3. **Claude queries**: Databricks and shows results + reasoning
4. **Iterate**: Back and forth until satisfied
5. **You say**: "Punch it" 
6. **Claude creates**: Final notebook `notebooks/XX-topic.ipynb` with all code + docs
7. **You verify**: Run the notebook

See [docs/eda-volleying-with-claude.md](docs/eda-volleying-with-claude.md) for details.

## Project Structure

```
.
├── .env                    # Your credentials (DO NOT COMMIT)
├── pyproject.toml          # uv dependencies
├── requirements.txt        # pip fallback
├── utils/
│   ├── databricks_query.py # Query client (supports SELECT, SHOW, DESCRIBE, WITH)
│   └── token_auth_setup.py # Token management
├── notebooks/
│   ├── temp_code/          # Volleying code goes here
│   └── *.ipynb            # Final notebooks
└── docs/
    └── eda-volleying-with-claude.md  # Workflow guide
```

## Using the Query Client

```python
from utils.databricks_query import query_databricks

df = query_databricks("""
    SELECT manufacturer, COUNT(*) as count
    FROM my_table
    WHERE date >= '2025-08-01'
    GROUP BY manufacturer
""", query_name="Manufacturer Count")

print(df)
```

**Supported SQL**: SELECT, SHOW, DESCRIBE/DESC, WITH (CTEs)  
**Blocked**: INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE

## Key Features

- ✅ SQL injection protection
- ✅ Automatic .env loading
- ✅ Returns pandas DataFrames
- ✅ Supports DESCRIBE and WITH clauses
- ✅ Volleying workflow with Claude
- ✅ Jupytext QA validation

## Common Issues

**Path errors in notebooks:**
```python
# For .py scripts:
sys.path.insert(0, str(Path(__file__).parent.parent / 'utils'))

# For .ipynb notebooks:
sys.path.insert(0, str(Path.cwd().parent / 'utils'))
```

**Token expired:**
```bash
echo "sql" | python3 utils/token_auth_setup.py --refresh-token
```

**Data type issues:**
```python
df['col'] = pd.to_numeric(df['col'], errors='coerce')
```

## Using as a Template

```bash
git clone <this-repo> my-new-project
cd my-new-project
rm -rf .git && git init
cp .env.template .env
# Edit .env, then: uv sync
```

---

**That's it. Clone, setup .env, start volleying with Claude.**