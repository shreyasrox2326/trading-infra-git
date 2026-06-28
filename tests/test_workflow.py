from pathlib import Path


WORKFLOW = Path(".github/workflows/daily-paper.yml")


def _workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_daily_workflow_has_cron() -> None:
    text = _workflow_text()

    assert "schedule:" in text
    assert 'cron: "30 13 * * 1-5"' in text


def test_daily_workflow_refresh_runs_before_paper() -> None:
    text = _workflow_text()

    assert text.index("market-data-refresh") < text.index("paper-dry-run")
    assert "refresh_status" in text
    assert 'refresh_status}" = "no_data"' in text


def test_daily_workflow_uses_github_secrets() -> None:
    text = _workflow_text()

    assert "secrets.R2_ACCESS_KEY_ID" in text
    assert "secrets.R2_SECRET_ACCESS_KEY" in text
    assert "secrets.R2_S3_API" in text
    assert "secrets.R2_BUCKET_NAME" in text
    assert "R2_ACCESS_KEY_ID=" not in text
    assert "R2_SECRET_ACCESS_KEY=" not in text


def test_daily_workflow_manual_override_inputs_exist() -> None:
    text = _workflow_text()

    assert "workflow_dispatch:" in text
    assert "run_date:" in text
    assert "exchange:" in text
    assert "skip_market_refresh:" in text
    assert "upload_results:" in text
