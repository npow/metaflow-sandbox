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
    assert "value = env.get(key)" in source
    assert "os.environ[key] = value" in source


def test_fallback_to_metaflow_namespaced_backend_keys() -> None:
    source = CLI_FILE.read_text()
    assert 'os.environ.get("METAFLOW_DAYTONA_API_KEY", "")' in source
    assert 'os.environ.get("METAFLOW_E2B_API_KEY", "")' in source


def test_forces_local_metadata_inside_service_mode() -> None:
    source = CLI_FILE.read_text()
    assert 'if ctx.obj.metadata.TYPE == "service":' in source
    assert 'top_params["metadata"] = "local"' in source


def test_has_code_package_local_path_option() -> None:
    source = CLI_FILE.read_text()
    assert "--code-package-local-path" in source


def test_has_deps_staging_dir_option() -> None:
    source = CLI_FILE.read_text()
    assert "--deps-staging-dir" in source


def test_creates_tarball_stager_from_local_path() -> None:
    source = CLI_FILE.read_text()
    assert "TarballStager" in source
    assert "code_package_local_path" in source


def test_creates_conda_offline_installer_from_staged() -> None:
    source = CLI_FILE.read_text()
    assert "CondaOfflineInstaller" in source
    assert "from_staged" in source
    assert "deps_staging_dir" in source


def test_installer_load_failure_is_nonfatal() -> None:
    """CondaOfflineInstaller load failure must be caught and logged."""
    source = CLI_FILE.read_text()
    assert "Failed to load dep installer" in source
    assert "Falling back to bootstrap_commands" in source


def test_passes_stager_and_installer_to_executor() -> None:
    source = CLI_FILE.read_text()
    assert "stager=stager" in source
    assert "installer=installer" in source
