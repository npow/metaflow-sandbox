"""Structural tests for sandbox_cli metadata sync/replay hooks."""

from pathlib import Path


CLI_FILE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "metaflow_extensions"
    / "sandbox"
    / "plugins"
    / "sandbox_cli.py"
)


def test_supports_force_local_metadata_sync() -> None:
    source = CLI_FILE.read_text()
    assert 'ctx.obj.metadata.TYPE in ("local", "service")' in source
    assert "sync_local_metadata_from_datastore" in source


def test_supports_service_replay_from_local_metadata() -> None:
    source = CLI_FILE.read_text()
    assert "_replay_task_metadata_to_service" in source
    assert 'base_url + "/metadata"' in source
    assert 'base_url + "/artifact"' in source


def test_sets_backend_auth_env_vars_before_launch() -> None:
    source = CLI_FILE.read_text()
    assert 'for key in ("DAYTONA_API_KEY", "DAYTONA_API_URL", "E2B_API_KEY")' in source
    assert "os.environ[key] = env[key]" in source


def test_fallback_to_metaflow_namespaced_backend_keys() -> None:
    source = CLI_FILE.read_text()
    assert 'os.environ.get("METAFLOW_DAYTONA_API_KEY", "")' in source
    assert 'os.environ.get("METAFLOW_E2B_API_KEY", "")' in source
