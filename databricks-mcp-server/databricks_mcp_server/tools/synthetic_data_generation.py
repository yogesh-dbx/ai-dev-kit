"""
MCP Tool Wrappers for Synthetic Data Generation

Wraps databricks-mcp-core synthetic data generation functions as MCP tools.
"""
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../databricks-mcp-core"))

from databricks_mcp_core.client import DatabricksClient
from databricks_mcp_core.synthetic_data_generation import (
    get_template,
    generate_and_upload_on_cluster
)
from databricks_mcp_core.spark_declarative_pipelines import workspace_files


# Lazy client initialization
_client = None

def get_client():
    """
    Get or create Databricks client.

    Uses DATABRICKS_CONFIG_PROFILE env var if set, otherwise falls back to
    DATABRICKS_HOST/DATABRICKS_TOKEN env vars.
    """
    global _client
    if _client is None:
        _client = DatabricksClient()
    return _client


def get_synth_data_template_tool(arguments: dict) -> dict:
    """MCP tool: Get generate_data.py template"""
    try:
        result = get_template(
            template_type=arguments.get("template_type", "story"),
            template_root=arguments.get("template_root")
        )
        return {
            "content": [{
                "type": "text",
                "text": f"📄 Template: {arguments.get('template_type', 'story')}\n\n"
                       f"```python\n{result['code']}\n```\n\n"
                       f"Source: {result['source_path']}"
            }]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error: {str(e)}"}],
            "isError": True
        }


def write_synth_data_script_to_workspace_tool(arguments: dict) -> dict:
    """MCP tool: Write generate_data.py script to Databricks workspace"""
    try:
        client = get_client()
        workspace_files.write_file(
            client,
            arguments["workspace_path"],
            arguments["code"],
            language="PYTHON",
            overwrite=arguments.get("overwrite", True)
        )
        return {
            "content": [{
                "type": "text",
                "text": f"✅ Script written to workspace\n\nPath: {arguments['workspace_path']}\n"
                       f"Overwrite: {arguments.get('overwrite', True)}"
            }]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error: {str(e)}"}],
            "isError": True
        }


def generate_and_upload_synth_data_tool(arguments: dict) -> dict:
    """MCP tool: Execute generate_data.py on cluster and write to Volume"""
    try:
        client = get_client()
        result = generate_and_upload_on_cluster(
            client,
            arguments["cluster_id"],
            arguments["workspace_path"],
            arguments["catalog"],
            arguments["schema"],
            arguments["volume"],
            arguments.get("scale_factor", 1.0),
            arguments.get("remote_subfolder", "incoming_data"),
            arguments.get("clean", True),
            arguments.get("timeout_sec", 600)
        )

        status = "✅" if result["success"] else "❌"
        text = f"{status} Synthetic Data Generation\n\n"
        text += f"Cluster: {arguments['cluster_id']}\n"
        text += f"Volume: {result['volume_path']}\n"
        text += f"Duration: {result['duration_sec']:.2f}s\n\n"

        if result.get("error"):
            text += f"❌ Error: {result['error']}\n\n"

        text += f"Output:\n{result['output']}"

        return {
            "content": [{"type": "text", "text": text}],
            "isError": not result["success"]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error: {str(e)}"}],
            "isError": True
        }


# Tool handler mapping
TOOL_HANDLERS = {
    "get_synth_data_template": get_synth_data_template_tool,
    "write_synth_data_script_to_workspace": write_synth_data_script_to_workspace_tool,
    "generate_and_upload_synth_data": generate_and_upload_synth_data_tool,
}


def get_tool_definitions():
    """Return MCP tool definitions for synthetic data generation"""
    return [
        {
            "name": "get_synth_data_template",
            "description": "Get a generate_data.py template for synthetic data generation. Choose 'story' for reference implementation or 'empty' for scaffold.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "template_type": {
                        "type": "string",
                        "enum": ["story", "empty"],
                        "description": "Template type: 'story' (reference implementation) or 'empty' (scaffold)",
                        "default": "story"
                    },
                    "template_root": {
                        "type": "string",
                        "description": "Optional override for template folder root"
                    }
                }
            }
        },
        {
            "name": "write_synth_data_script_to_workspace",
            "description": "Write generate_data.py script to Databricks workspace. This stores the script in the workspace so it can be executed on a cluster.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workspace_path": {
                        "type": "string",
                        "description": "Workspace path (e.g., '/Workspace/Users/user@example.com/generate_data.py')"
                    },
                    "code": {
                        "type": "string",
                        "description": "Python code content for generate_data.py"
                    },
                    "overwrite": {
                        "type": "boolean",
                        "description": "Overwrite if file exists",
                        "default": True
                    }
                },
                "required": ["workspace_path", "code"]
            }
        },
        {
            "name": "generate_and_upload_synth_data",
            "description": "Execute generate_data.py on Databricks cluster and write output directly to Unity Catalog Volume. The script runs on the cluster and writes parquet files to the specified volume path.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "Databricks cluster ID"
                    },
                    "workspace_path": {
                        "type": "string",
                        "description": "Workspace path to generate_data.py script"
                    },
                    "catalog": {
                        "type": "string",
                        "description": "Unity Catalog name"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name"
                    },
                    "volume": {
                        "type": "string",
                        "description": "Volume name"
                    },
                    "scale_factor": {
                        "type": "number",
                        "description": "Scale factor for data generation (multiplier for row counts)",
                        "default": 1.0
                    },
                    "remote_subfolder": {
                        "type": "string",
                        "description": "Subfolder within volume for output",
                        "default": "incoming_data"
                    },
                    "clean": {
                        "type": "boolean",
                        "description": "Clean target folder before generation",
                        "default": True
                    },
                    "timeout_sec": {
                        "type": "integer",
                        "description": "Execution timeout in seconds",
                        "default": 600
                    }
                },
                "required": ["cluster_id", "workspace_path", "catalog", "schema", "volume"]
            }
        }
    ]
