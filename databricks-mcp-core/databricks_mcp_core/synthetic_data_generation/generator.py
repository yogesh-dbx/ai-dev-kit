"""
Synthetic Data Generation - Local Parquet → Databricks Volume

Functions to manage synthetic data script templates, execution, and uploading
generated parquet files to a Unity Catalog Volume.
"""
import os
import time
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, List

# Match your existing client import pattern (HTTP wrapper or SDK adapter)
from ..client import DatabricksClient


# ---------------------------
# Templates
# ---------------------------
def get_template(template_type: str = "story",
                 template_root: Optional[str] = None) -> Dict[str, Any]:
    """
    Get a generate_data.py template.

    Args:
        template_type: "story" (reference implementation) or "empty" (scaffold)
        template_root: Optional override for template folder root. If None, uses
                       repo-relative defaults: "project_template" or "project_template_empty"

    Returns:
        {"code": str, "source_path": str}

    Raises:
        FileNotFoundError: If template file not found
        OSError: On read failures
    """
    if template_type not in {"story", "empty"}:
        raise ValueError("template_type must be 'story' or 'empty'")

    base_dir = Path(template_root) if template_root else Path(".")
    tpl_dir = (base_dir / "project_template") if template_type == "story" else (base_dir / "project_template_empty")
    source_path = tpl_dir / "generate_data.py"

    code = source_path.read_text(encoding="utf-8")
    return {"code": code, "source_path": str(source_path)}


def write_script(project_path: str,
                 code: str,
                 overwrite: bool = True) -> Dict[str, Any]:
    """
    Write generate_data.py into a project directory.

    Args:
        project_path: Absolute or relative path to project folder
                      (will write to <project_path>/generate_data.py)
        code: Python code content
        overwrite: If False and file exists, raises FileExistsError

    Returns:
        {"path": str, "bytes_written": int}

    Raises:
        FileExistsError: If file exists and overwrite is False
        OSError: On write failures
    """
    proj = Path(project_path)
    proj.mkdir(parents=True, exist_ok=True)
    target = proj / "generate_data.py"

    if target.exists() and not overwrite:
        raise FileExistsError(f"{target} already exists; set overwrite=True to replace")

    bytes_written = target.write_text(code, encoding="utf-8")
    return {"path": str(target), "bytes_written": bytes_written}


# ---------------------------
# Execution
# ---------------------------
def execute_script(project_path: str,
                   scale_factor: float = 1.0,
                   python_bin: Optional[str] = None,
                   timeout_sec: int = 300,
                   env: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Execute generate_data.py to produce parquet files locally.

    Writes under <project_path>/data/<table_name>/*.parquet

    Args:
        project_path: Folder containing generate_data.py
        scale_factor: Sets SCALE_FACTOR env var for the script
        python_bin: Optional Python executable (defaults to `sys.executable`)
        timeout_sec: Subprocess timeout in seconds
        env: Extra environment vars to merge

    Returns:
        {
          "exit_code": int,
          "stdout": str,
          "stderr": str,
          "duration_sec": float,
          "log_path": str
        }

    Raises:
        FileNotFoundError: If generate_data.py is missing
        subprocess.TimeoutExpired: On timeout
        OSError: On spawn errors
    """
    proj = Path(project_path)
    script = proj / "generate_data.py"
    if not script.exists():
        raise FileNotFoundError(f"Missing {script}")

    py = python_bin or os.environ.get("PYTHON_BIN") or None
    cmd = ([py] if py else ["python"]) + ["-u", "generate_data.py"]

    env_vars = os.environ.copy()
    env_vars["PYTHONUNBUFFERED"] = "1"
    env_vars["PYTHONDONTWRITEBYTECODE"] = "1"
    env_vars["SCALE_FACTOR"] = str(scale_factor)
    if env:
        env_vars.update(env)

    log_path = proj / "generation.log"
    start = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(proj),
            env=env_vars,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as e:
        return {
            "exit_code": -1,
            "stdout": (e.stdout or ""),
            "stderr": (e.stderr or f"Timed out after {timeout_sec}s"),
            "duration_sec": time.time() - start,
            "log_path": str(log_path),
        }

    # Persist logs
    try:
        log_path.write_text(
            f"Exit Code: {proc.returncode}\n\n=== STDOUT ===\n{proc.stdout}\n\n=== STDERR ===\n{proc.stderr}",
            encoding="utf-8",
        )
    except Exception:
        pass

    return {
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "duration_sec": time.time() - start,
        "log_path": str(log_path),
    }


# ---------------------------
# Upload to Volume
# ---------------------------
def upload_to_volume(client: DatabricksClient,
                     catalog: str,
                     schema: str,
                     volume: str,
                     local_data_dir: str,
                     remote_subfolder: str = "incoming_data",
                     clean: bool = True,
                     max_files: Optional[int] = None) -> Dict[str, Any]:
    """
    Upload generated parquet files to a Unity Catalog Volume.

    Mirrors local folder structure under:
      /Volumes/{catalog}/{schema}/{volume}/{remote_subfolder}/...

    Args:
        client: Databricks client instance
        catalog: Unity Catalog name
        schema: Schema name
        volume: Volume name
        local_data_dir: Local path containing table folders with *.parquet
        remote_subfolder: Base folder under the volume (e.g., 'incoming_data')
        clean: If True, removes remote_subfolder before upload
        max_files: Optional cap for number of files (debugging)

    Returns:
        {
          "success": bool,
          "files_uploaded": int,
          "uploaded_paths": List[str],
          "errors": List[str],
          "remote_base": str
        }

    Raises:
        requests.HTTPError: If API requests fail inside DatabricksClient
        FileNotFoundError: If local_data_dir does not exist
        OSError: On local I/O errors
    """
    src = Path(local_data_dir)
    if not src.exists():
        raise FileNotFoundError(f"No local data directory: {src}")

    remote_base = f"/Volumes/{catalog}/{schema}/{volume}/{remote_subfolder}"

    # Clean remote folder if requested (requires corresponding client helpers)
    if clean:
        client.files.delete_directory(remote_base, ignore_missing=True)
        client.files.create_directory(remote_base)

    uploaded_paths: List[str] = []
    errors: List[str] = []
    files_seen = 0

    # Walk local files and upload
    for root, _, files in os.walk(src):
        for fname in files:
            if max_files is not None and files_seen >= max_files:
                break
            if not fname.lower().endswith(".parquet"):
                continue

            local_file = Path(root) / fname
            rel_path = os.path.relpath(local_file, src).replace("\\", "/")
            remote_path = f"{remote_base}/{rel_path}"
            remote_dir = str(Path(remote_path).parent).replace("\\", "/")

            try:
                client.files.create_directory(remote_dir)
                with open(local_file, "rb") as f:
                    data = f.read()
                client.files.upload(remote_path, data=data, overwrite=True)
                uploaded_paths.append(remote_path)
                files_seen += 1
            except Exception as e:
                errors.append(f"{rel_path}: {e}")

    return {
        "success": len(errors) == 0,
        "files_uploaded": len(uploaded_paths),
        "uploaded_paths": uploaded_paths,
        "errors": errors,
        "remote_base": remote_base,
    }


# ---------------------------
# Orchestration
# ---------------------------
def generate_and_upload(client: DatabricksClient,
                        project_path: str,
                        catalog: str,
                        schema: str,
                        volume: str,
                        scale_factor: float = 1.0,
                        remote_subfolder: str = "incoming_data",
                        clean: bool = True,
                        python_bin: Optional[str] = None,
                        timeout_sec: int = 300) -> Dict[str, Any]:
    """
    One-shot orchestration: execute local generate_data.py then upload parquet to a volume.

    Args:
        client: Databricks client instance
        project_path: Project directory containing generate_data.py
        catalog: Unity Catalog name
        schema: Schema name
        volume: Volume name
        scale_factor: Sets SCALE_FACTOR for generation run
        remote_subfolder: Base folder under the volume (e.g., 'incoming_data')
        clean: If True, remote_subfolder is cleaned before upload
        python_bin: Optional Python executable
        timeout_sec: Subprocess timeout

    Returns:
        {
          "generation": {...execute_script result...},
          "upload": {...upload_to_volume result...}
        }

    Raises:
        Exceptions from execute_script or upload_to_volume
    """
    gen = execute_script(
        project_path=project_path,
        scale_factor=scale_factor,
        python_bin=python_bin,
        timeout_sec=timeout_sec,
    )
    if gen["exit_code"] != 0:
        return {"generation": gen, "upload": None}

    data_dir = str(Path(project_path) / "data")
    upl = upload_to_volume(
        client=client,
        catalog=catalog,
        schema=schema,
        volume=volume,
        local_data_dir=data_dir,
        remote_subfolder=remote_subfolder,
        clean=clean,
    )
    return {"generation": gen, "upload": upl}


# ---------------------------
# Cluster-Based Execution
# ---------------------------
def execute_script_on_cluster(
    client: DatabricksClient,
    cluster_id: str,
    workspace_path: str,
    volume_output_path: str,
    scale_factor: float = 1.0,
    timeout_sec: int = 600
) -> Dict[str, Any]:
    """
    Execute generate_data.py on Databricks cluster, writing directly to Volume.

    Args:
        client: Databricks client instance
        cluster_id: Databricks cluster ID
        workspace_path: Workspace path to generate_data.py (e.g., "/Workspace/Users/user@example.com/generate_data.py")
        volume_output_path: Volume path for output (e.g., "/Volumes/catalog/schema/volume/incoming_data")
        scale_factor: Sets SCALE_FACTOR env var for the script
        timeout_sec: Execution timeout in seconds

    Returns:
        {
          "success": bool,
          "output": str,
          "error": Optional[str],
          "duration_sec": float
        }

    Raises:
        Exceptions from compute execution or workspace file operations
    """
    from ..compute import execution
    from ..spark_declarative_pipelines import workspace_files

    # Read script from workspace
    script_code = workspace_files.read_file(client, workspace_path)

    # Wrap to set environment variables and execute
    wrapped_code = f"""
import os
os.environ['SCALE_FACTOR'] = '{scale_factor}'
os.environ['OUTPUT_PATH'] = '{volume_output_path}'

{script_code}
"""

    # Execute on cluster
    result = execution.execute_databricks_command(
        client=client,
        cluster_id=cluster_id,
        language="python",
        code=wrapped_code,
        timeout_sec=timeout_sec
    )

    return result


def generate_and_upload_on_cluster(
    client: DatabricksClient,
    cluster_id: str,
    workspace_path: str,
    catalog: str,
    schema: str,
    volume: str,
    scale_factor: float = 1.0,
    remote_subfolder: str = "incoming_data",
    clean: bool = True,
    timeout_sec: int = 600
) -> Dict[str, Any]:
    """
    One-shot orchestration: execute generate_data.py on cluster and write directly to Volume.

    Args:
        client: Databricks client instance
        cluster_id: Databricks cluster ID
        workspace_path: Workspace path to generate_data.py
        catalog: Unity Catalog name
        schema: Schema name
        volume: Volume name
        scale_factor: Sets SCALE_FACTOR for generation run
        remote_subfolder: Base folder under the volume (e.g., 'incoming_data')
        clean: If True, remote_subfolder is cleaned before execution
        timeout_sec: Execution timeout

    Returns:
        {
          "success": bool,
          "output": str,
          "volume_path": str,
          "error": Optional[str],
          "duration_sec": float
        }

    Raises:
        Exceptions from cluster execution or Files API operations
    """
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{remote_subfolder}"

    # Clean if requested
    if clean:
        try:
            client.files.delete_directory(volume_path, ignore_missing=True)
            client.files.create_directory(volume_path)
        except Exception as e:
            return {
                "success": False,
                "output": "",
                "volume_path": volume_path,
                "error": f"Failed to clean volume path: {str(e)}",
                "duration_sec": 0.0
            }

    # Execute on cluster
    result = execute_script_on_cluster(
        client, cluster_id, workspace_path, volume_path, scale_factor, timeout_sec
    )
    result["volume_path"] = volume_path
    return result