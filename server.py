import logging
import os
from typing import Any, List, Dict
import polars as pl

from mcp.server.fastmcp import FastMCP


# Get project root directory path for log file path.
# When using the stdio transmission method,
# relative paths may cause log files to fail to create
# due to the client's running location and permission issues,
# resulting in the program not being able to run.
# Thus using os.path.join(ROOT_DIR, "excel-mcp.log") instead.

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_FILE = os.path.join(ROOT_DIR, "polars-mcp.log")

# Initialize FILES_PATH variable without assigning a value
FILES_PATH = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Referring to https://github.com/modelcontextprotocol/python-sdk/issues/409#issuecomment-2816831318
        # The stdio mode server MUST NOT write anything to its stdout that is not a valid MCP message.
        logging.FileHandler(LOG_FILE)
    ],
)
logger = logging.getLogger("polars-mcp")
# Initialize FastMCP server
mcp = FastMCP(
    "polars-mcp",
    version="0.1.0",
    description="Polars MCP Server for manipulating CSV files",
    env_vars={
        "FILES_PATH": {
            "description": "Path to csv files directory",
            "required": False,
            "default": FILES_PATH,
        }
    },
)


def get_path(filename: str) -> str:
    """Get full path to file.

    Args:
        filename: Name of file

    Returns:
        Full path to file
    """
    # If filename is already an absolute path, return it
    if os.path.isabs(filename):
        return filename

    # Check if in SSE mode (FILES_PATH is not None)
    if FILES_PATH is None:
        # Must use absolute path
        raise ValueError(
            f"Invalid filename: {filename}, must be an absolute path when not in SSE mode"
        )

    # In SSE mode, if it's a relative path, resolve it based on EXCEL_FILES_PATH
    return os.path.join(FILES_PATH, filename)


@mcp.tool()
def describe_csv(
    filepath: str,
) -> Dict[str, Any]:
    """Describe a CSV file.

    Args:
        filepath: Path to the CSV file

    Returns:
        A dictionary containing the description of the CSV file
    """
    try:
        full_path = get_path(filepath)
        df = pl.read_csv(full_path, infer_schema_length=10000)

        description = {
            # "columns": list(df.columns),
            "shape": df.shape,
            "dtypes": {col: str(df[col].dtype) for col in df.columns},
            # "head": df.head(1).to_dict(orient="records"),
        }
        return description
    except Exception as e:
        logger.error(f"Error describing CSV file: {e}")
        raise


@mcp.tool()
def execute_queries(
    filepath: str,
    queries: List[str],
) -> List[Dict[str, Any]]:
    """Execute SQL queries on a CSV file with Polars.
    the df is identified as "self" in the SQL queries.

    Args:
        filepath: Path to the CSV file
        queries: List of SQL queries to execute (df is identified as "self")

    Returns:
        A list of dictionaries containing the results of each query
    """
    try:
        full_path = get_path(filepath)
        df = pl.read_csv(full_path, infer_schema_length=10000)
        results = []
        for query in queries:
            result = df.sql(query)
            results.append(result.to_dict())
        return results
    except Exception as e:
        logger.error(f"Error executing queries: {e}")
        raise


@mcp.tool()
def execute_query(
    filepath: str,
    query: str,
) -> Dict[str, Any]:
    """Execute a single SQL query on a CSV file with Polars.

    Args:
        filepath: Path to the CSV file
        query: SQL query to execute (df is identified as "self")

    Returns:
        A dictionary containing the result of the query
    """
    try:
        full_path = get_path(filepath)
        df = pl.read_csv(full_path, infer_schema_length=10000)
        result = df.sql(query)
        # Convert result to a serializable format
        if isinstance(result, pl.DataFrame):
            return result.to_dict(as_series=False)
        elif isinstance(result, pl.Series):
            return result.to_list()
        else:
            return result
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise


async def run_sse():
    """Run Polars MCP server in SSE mode."""
    # Assign value to FILES_PATH in SSE mode
    global FILES_PATH
    FILES_PATH = os.environ.get("FILES_PATH", "./files")
    # Create directory if it doesn't exist
    os.makedirs(FILES_PATH, exist_ok=True)

    try:
        logger.info(
            f"Starting Polars MCP server with SSE transport (files directory: {FILES_PATH})"
        )
        await mcp.run_sse_async()
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        await mcp.shutdown()
    except Exception as e:
        logger.error(f"Server failed: {e}")
        raise
    finally:
        logger.info("Server shutdown complete")


def run_stdio():
    """Run Polars MCP server in stdio mode."""
    # No need to assign FILES_PATH in stdio mode

    try:
        logger.info("Starting Polars MCP server with stdio transport")
        mcp.run(transport="stdio")
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server failed: {e}")
        raise
    finally:
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    run_stdio()
