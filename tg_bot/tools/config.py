import os


def require_env(name: str) -> str:
    value = (os.environ.get(name) or '').strip()
    if not value:
        raise RuntimeError(f'Environment variable {name} is not set')
    return value
