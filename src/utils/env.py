from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Any
from pathlib import Path
import yaml

@dataclass(frozen=True)
class EnvConfig:
    catalog: str
    project: str
    landing_base_path: str
    raw_base_path: str
    curated_base_path: str
    ops_base_path: str
    checkpoint_base_path: str
    logger_level : str
    allowed_datasets: list[str]
    dq_scope : list[str]
    backfill_days: int


def _require_str(value, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing required config values: {name}")
    return value

def load_envs() -> EnvConfig:
    env = os.getenv("ENV", "dev").strip().lower()

    cfg_file = Path(__file__).resolve().parents[1] / "configs" / f"{env}.yaml"
    
    if not cfg_file.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_file}")
    
    with cfg_file.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    catalog = cfg.get("catalog")
    project = cfg.get("project")
    logger_level = cfg.get("logger_level")
    paths: dict[str, Any] = cfg.get("paths", {})

    
    allowed_datasets = cfg.get("allowed_datasets")
    if (
        not isinstance(allowed_datasets, list)
        or not allowed_datasets
        or not all(isinstance(i, str) and i for i in allowed_datasets)
    ):
        raise ValueError("allowed_datasets must be a non-empty list of strings")
    
    dq_scope = cfg.get("dq_scope")
    if (
        not isinstance(dq_scope, list)
        or not dq_scope
        or not all(isinstance(i, str) and i for i in allowed_datasets)
    ):
        raise ValueError("dq_scope must be a non-empty list of strings")
    
    backfill_days = cfg.get("backfill_days", 0)

    return EnvConfig(
        catalog=_require_str(catalog, "catalog"),
        project=_require_str(project, "project"),
        landing_base_path=_require_str(paths.get("landing_base_path"), "landing_base_path"),
        raw_base_path=_require_str(paths.get("raw_base_path"), "raw_base_path"),
        curated_base_path=_require_str(paths.get("curated_base_path"), "curated_base_path"),
        ops_base_path=_require_str(paths.get("ops_base_path"), "ops_base_path"),
        checkpoint_base_path=_require_str(paths.get("checkpoint_base_path"), "checkpoint_base_path"),
        logger_level=_require_str(logger_level, "logger_level"),
        allowed_datasets=allowed_datasets,
        dq_scope=dq_scope,
        backfill_days=backfill_days
    )