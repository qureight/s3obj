from pathlib import Path


def get_extension(path: str):
    return "".join(Path(path).suffixes)