def validate_file_path(file_path: str) -> bool:
    """Validate the given file path."""
    import os
    return os.path.isfile(file_path)

def read_file(file_path: str) -> str:
    """Read the contents of a file."""
    with open(file_path, 'r') as file:
        return file.read()

def write_file(file_path: str, data: str) -> None:
    """Write data to a file."""
    with open(file_path, 'w') as file:
        file.write(data)

def delete_file(file_path: str) -> None:
    """Delete a file."""
    import os
    if os.path.isfile(file_path):
        os.remove(file_path)