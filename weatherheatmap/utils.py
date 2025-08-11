from pathlib import Path
from typing import List


def list_files(folder: str, file_pattern: str = "*.csv.gz") -> List[Path]:
    """Glob search file pattern in the folder."""
    file_list = list(Path(folder).glob(file_pattern))
    print(f"Found {len(file_list)} files to process in {folder}...")
    return file_list
