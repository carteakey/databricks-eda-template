# Volleying with Claude Code for Data Analytics EDA

## approach
- when I say "volley" I want claude code to use the tools in utils/ to run queries in databricks, look at the returned data, try to understand it, including any gaps in what we thought it would have returned, reason with it and show me the output and its reasoning
- Before starting refresh the token by running token auth setup with refresh-token param.

```
echo "sql" | python3 utils/token_auth_setup.py --refresh-token
```

- Test connection and then start. No alarms and no surprises.
```
python3 utils/token_auth_setup.py --test-connection
```
- Copilot can put this temp code in notebooks/temp_code/[0-9]{2}
-<filename>.py files where the first two digits match the notebook prefix we will be working on
- Sample code is present in temp_code directory to make things easier  (01-initial_dataset_exploration.py)
- i would then ask it to try a few things based on the data returned and it would write more temp code..
- We will go back and forth until I tell it to "punch it"
- at this command,
    - it would look back at our back and forth,
    - look at the temp code files and then
    - write up or update the [0-9]{2}-<filename>.ipynb complete with
        - the code blocks
        - followed by markdown text blocks
        - which document the progressive code and analysis that was performed during the volleying and
        - the insights gained from the reasoning
    - when done with the notebook, it would return back to me
- I would execute the notebook and confirm all is working and this would be the end of the specific EDA cycle

- Sample
```
# Add utils to path for importing - use absolute path to avoid issues
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
utils_path = os.path.join(project_root, 'utils')
sys.path.insert(0, utils_path)

from databricks_query import DatabricksQueryClient, query_databricks
```


## quality assurance step
- after creating the notebook, Copilot should use jupytext to convert it to .py format and run the .py version to check for errors
- common issues to fix:
    - **path issues for Python scripts**: use `Path(__file__).parent.parent / 'utils'` for .py files
    - **path issues for Jupyter notebooks**: use `Path.cwd().parent / 'utils'` for .ipynb files (since `__file__` is not available in notebooks)
    - **data type issues**: pandas DataFrames from Databricks may return object/string types, use `pd.to_numeric()` for calculations
    - **import issues**: ensure all required libraries are properly imported
- **CRITICAL**: after fixing errors in the .py version, manually update the notebook to use notebook-compatible paths (`Path.cwd().parent / 'utils'`)
- **VERIFY ALL CELLS**: check the entire notebook for ANY remaining `__file__` references - jupytext may create duplicate cells or leave problematic code in markdown cells
- use `grep -n "__file__" notebooks/filename.ipynb` to verify no remaining instances
- this ensures both the .py script works for testing AND the notebook works correctly when the user executes it
- **remember**: `.py` scripts need `__file__` paths, `.ipynb` notebooks need `cwd()` paths

## common jupytext issues to watch for
- **duplicate cells**: jupytext conversion may create multiple import cells, remove duplicates
- **markdown cells with code**: code aCopilotidentally placed in markdown cells instead of code cells
- **mixed path types**: some cells may still have `__file__` while others have `cwd()` - ensure consistency
- **always validate**: run `grep` commands to verify all `__file__` references are removed from the final notebook
