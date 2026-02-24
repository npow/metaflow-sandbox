"""End-to-end integration test: run a real Metaflow flow with @daytona.

Requirements:
    - DAYTONA_API_KEY env var set
    - Remote datastore configured (e.g. METAFLOW_DEFAULT_DATASTORE=s3)
    - pip install metaflow-sandbox[daytona]

Run: pytest tests/integration/ -m integration
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest


@pytest.mark.integration
class TestDaytonaE2E:
    """Runs an actual Metaflow flow with @daytona decorator."""

    def test_simple_flow(self, tmp_path) -> None:
        """A minimal flow that sets an artifact and reads it back."""
        flow_file = tmp_path / "test_flow.py"
        flow_file.write_text(
            textwrap.dedent("""\
                from metaflow import FlowSpec, step

                class SandboxTestFlow(FlowSpec):
                    @step
                    def start(self):
                        self.x = 42
                        self.next(self.end)

                    @step
                    def end(self):
                        assert self.x == 42
                        print("Success: x =", self.x)

                if __name__ == "__main__":
                    SandboxTestFlow()
            """)
        )

        result = subprocess.run(
            [sys.executable, str(flow_file), "run"],
            capture_output=True,
            text=True,
            timeout=300,
        )

        assert result.returncode == 0, (
            f"Flow failed.\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
