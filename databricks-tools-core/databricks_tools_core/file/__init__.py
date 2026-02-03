"""
File - Workspace and Volume File Operations

Functions for uploading files and folders to Databricks Workspace and Unity Catalog Volumes.
"""

from .workspace import (
    UploadResult,
    FolderUploadResult,
    upload_folder,
    upload_file,
)

from .volume_files import (
    VolumeFileInfo,
    VolumeUploadResult,
    VolumeDownloadResult,
    list_volume_files,
    upload_to_volume,
    download_from_volume,
    delete_volume_file,
    delete_volume_directory,
    create_volume_directory,
    get_volume_file_metadata,
)

__all__ = [
    # Workspace file operations
    "UploadResult",
    "FolderUploadResult",
    "upload_folder",
    "upload_file",
    # Volume file operations
    "VolumeFileInfo",
    "VolumeUploadResult",
    "VolumeDownloadResult",
    "list_volume_files",
    "upload_to_volume",
    "download_from_volume",
    "delete_volume_file",
    "delete_volume_directory",
    "create_volume_directory",
    "get_volume_file_metadata",
]
