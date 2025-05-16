#!/usr/bin/env python3
"""
FraudIQ Project Cleanup Script
------------------------------
This script removes unnecessary files and directories from the FraudIQ project,
such as Python cache files, macOS system files, and temporary files.
"""

import os
import shutil
import fnmatch
from pathlib import Path

def main():
    print("🧹 Cleaning up FraudIQ project...")
    
    # Define the project root directory
    project_root = Path('.')
    
    # Track statistics
    stats = {
        'cache_dirs': 0,
        'cache_files': 0,
        'empty_dirs': 0,
        'temp_files': 0,
        'log_files': 0,
        'system_files': 0
    }
    
    # Files and directories to remove
    patterns_to_remove = {
        'cache_dirs': ['__pycache__', '.pytest_cache', '.mypy_cache', '.ruff_cache'],
        'cache_files': ['*.pyc', '*.pyo', '*.pyd', '.coverage'],
        'temp_files': ['*.tmp', '*.bak', '*.swp', '*.swo', '*~'],
        'log_files': ['*.log'],
        'system_files': ['.DS_Store', 'Thumbs.db']
    }
    
    # Directories to exclude from cleaning
    exclude_dirs = ['.git', '.venv', 'venv', 'airflow/logs']
    
    # Remove Python cache directories
    print("Removing cache directories...")
    for pattern in patterns_to_remove['cache_dirs']:
        for path in project_root.glob(f'**/{pattern}'):
            if any(str(path).startswith(f'./{excl}') for excl in exclude_dirs):
                continue
            if path.is_dir():
                print(f"  Removing {path}")
                shutil.rmtree(path)
                stats['cache_dirs'] += 1
    
    # Walk through the project and remove unnecessary files
    print("Removing unnecessary files...")
    for root, dirs, files in os.walk(project_root):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if not any(os.path.join(root, d).startswith(f'./{excl}') for excl in exclude_dirs)]
        
        # Remove files matching patterns
        for file in files:
            file_path = os.path.join(root, file)
            
            # Cache files
            if any(fnmatch.fnmatch(file, pattern) for pattern in patterns_to_remove['cache_files']):
                os.remove(file_path)
                print(f"  Removing cache file: {file_path}")
                stats['cache_files'] += 1
            
            # Temporary files
            elif any(fnmatch.fnmatch(file, pattern) for pattern in patterns_to_remove['temp_files']):
                os.remove(file_path)
                print(f"  Removing temporary file: {file_path}")
                stats['temp_files'] += 1
            
            # Log files (excluding Airflow logs)
            elif any(fnmatch.fnmatch(file, pattern) for pattern in patterns_to_remove['log_files']) and 'airflow/logs' not in file_path:
                os.remove(file_path)
                print(f"  Removing log file: {file_path}")
                stats['log_files'] += 1
            
            # System files
            elif any(fnmatch.fnmatch(file, pattern) for pattern in patterns_to_remove['system_files']):
                os.remove(file_path)
                print(f"  Removing system file: {file_path}")
                stats['system_files'] += 1
    
    # Remove empty directories (excluding those in the exclude list and airflow/plugins)
    print("Removing empty directories...")
    for root, dirs, files in os.walk(project_root, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if not os.listdir(dir_path) and not any(dir_path.startswith(f'./{excl}') for excl in exclude_dirs) and 'airflow/plugins' not in dir_path:
                os.rmdir(dir_path)
                print(f"  Removing empty directory: {dir_path}")
                stats['empty_dirs'] += 1
    
    print("\n✅ Cleanup complete!")
    print(f"Removed:")
    print(f"  - {stats['cache_dirs']} cache directories")
    print(f"  - {stats['cache_files']} cache files")
    print(f"  - {stats['empty_dirs']} empty directories")
    print(f"  - {stats['temp_files']} temporary files")
    print(f"  - {stats['log_files']} log files")
    print(f"  - {stats['system_files']} system files")

if __name__ == "__main__":
    main() 