"""Load config/config.yaml and merge in secrets from .env."""
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_config(config_path: str = None) -> dict:
    """Load YAML config and make sure .env is loaded so downstream code can read
    POSTGRES_PASSWORD etc. via os.environ. We deliberately do NOT interpolate secrets
    into the returned dict -- code that needs the password should read it from the
    environment directly (see src/utils/db.py) so it never ends up printed or logged
    as part of a config object.
    """
    load_dotenv(PROJECT_ROOT / ".env")

    if config_path is None:
        config_path = PROJECT_ROOT / "config" / "config.yaml"

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    return cfg


def resolve_path(cfg: dict, key_path: str) -> str:
    """Resolve a dotted config key (e.g. 'delta.bronze_path') to an absolute path
    relative to the project root, and make sure the parent directory exists.
    """
    node = cfg
    for key in key_path.split("."):
        node = node[key]
    p = Path(node)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)
