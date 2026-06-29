"""R2 usage inventory and budget guardrails."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trading_infra.storage.r2 import R2Client


DEFAULT_WARN_STORAGE_BYTES = 8 * 1024**3
DEFAULT_FAIL_STORAGE_BYTES = int(9.5 * 1024**3)
DEFAULT_WARN_CLASS_A_OPS = 800_000
DEFAULT_FAIL_CLASS_A_OPS = 950_000
DEFAULT_WARN_CLASS_B_OPS = 8_000_000
DEFAULT_FAIL_CLASS_B_OPS = 9_500_000


@dataclass(frozen=True)
class R2BudgetThresholds:
    warn_storage_bytes: int = DEFAULT_WARN_STORAGE_BYTES
    fail_storage_bytes: int = DEFAULT_FAIL_STORAGE_BYTES
    warn_class_a_operations: int = DEFAULT_WARN_CLASS_A_OPS
    fail_class_a_operations: int = DEFAULT_FAIL_CLASS_A_OPS
    warn_class_b_operations: int = DEFAULT_WARN_CLASS_B_OPS
    fail_class_b_operations: int = DEFAULT_FAIL_CLASS_B_OPS


def collect_r2_usage(
    client: R2Client,
    *,
    prefix: str = "",
    class_a_operations_month_to_date: int | None = None,
    class_b_operations_month_to_date: int | None = None,
) -> dict[str, Any]:
    """Collect object inventory usage from R2 listing."""
    objects = client.list_objects(prefix)
    storage_bytes = sum(int(item.get("size", 0)) for item in objects)
    return {
        "bucket": client.config.bucket,
        "prefix": prefix,
        "storage_bytes": storage_bytes,
        "object_count": len(objects),
        "class_a_operations_month_to_date": class_a_operations_month_to_date,
        "class_b_operations_month_to_date": class_b_operations_month_to_date,
        "estimated_free_tier_remaining": None,
        "estimated_monthly_cost": None,
    }


def apply_r2_budget(
    usage: dict[str, Any],
    *,
    thresholds: R2BudgetThresholds = R2BudgetThresholds(),
) -> dict[str, Any]:
    """Add warn/fail budget status to usage output."""
    fail_reasons = []
    warn_reasons = []
    if usage["storage_bytes"] > thresholds.fail_storage_bytes:
        fail_reasons.append("storage_bytes above failure threshold")
    elif usage["storage_bytes"] > thresholds.warn_storage_bytes:
        warn_reasons.append("storage_bytes above warning threshold")

    class_a = usage.get("class_a_operations_month_to_date")
    if class_a is not None:
        if class_a > thresholds.fail_class_a_operations:
            fail_reasons.append("Class A operations above failure threshold")
        elif class_a > thresholds.warn_class_a_operations:
            warn_reasons.append("Class A operations above warning threshold")

    class_b = usage.get("class_b_operations_month_to_date")
    if class_b is not None:
        if class_b > thresholds.fail_class_b_operations:
            fail_reasons.append("Class B operations above failure threshold")
        elif class_b > thresholds.warn_class_b_operations:
            warn_reasons.append("Class B operations above warning threshold")

    status = "fail" if fail_reasons else "warn" if warn_reasons else "ok"
    return {
        **usage,
        "status": status,
        "warn_reasons": warn_reasons,
        "fail_reasons": fail_reasons,
        "thresholds": thresholds.__dict__,
    }


def write_r2_usage_snapshot(report: dict[str, Any], *, output_dir: str | Path | None = None) -> Path:
    """Write a timestamped monthly R2 usage snapshot."""
    root = Path(output_dir) if output_dir else Path("data") / "import" / "audit" / "r2_usage"
    root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = root / f"r2_usage_{timestamp}.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return path
