"""End-to-end integration test: run a real Metaflow flow with @e2b.

Requirements:
    - E2B_API_KEY env var set
    - Remote datastore configured (e.g. METAFLOW_DEFAULT_DATASTORE=s3)
    - pip install metaflow-sandbox[e2b]

Run: pytest tests/integration/test_e2b_conda_pypi_e2e.py -m integration
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest


@pytest.mark.integration
class TestE2BCondaPypiE2E:
    """Runs a real E2B flow with both @pypi and @conda hydration."""

    def test_e2b_hydration_and_artifact_roundtrip(self, tmp_path) -> None:
        if not os.environ.get("E2B_API_KEY"):
            pytest.skip("E2B_API_KEY is not set")

        flow_file = tmp_path / "e2b_hydration_flow.py"
        flow_file.write_text(
            textwrap.dedent(
                """\
                from metaflow import FlowSpec, step, e2b, pypi, conda


                class E2BHydrationFlow(FlowSpec):
                    @step
                    def start(self):
                        self.raw = "hello_world"
                        self.next(self.pypi_step)

                    @e2b(cpu=1, memory=2048)
                    @pypi(packages={"pydash": "==8.0.5"})
                    @step
                    def pypi_step(self):
                        import pydash

                        self.camel = pydash.camel_case(self.raw)
                        self.next(self.conda_step)

                    @e2b(cpu=1, memory=2048)
                    @conda(libraries={"numpy": "1.26.4"}, python="3.12.12")
                    @step
                    def conda_step(self):
                        import numpy as np

                        self.total = int(np.array([1, 2, 3]).sum())
                        self.message = f"{self.camel}:{self.total}"
                        self.next(self.end)

                    @step
                    def end(self):
                        assert self.camel == "helloWorld", self.camel
                        assert self.total == 6, self.total
                        print("E2B_E2E_OK", self.message)


                if __name__ == "__main__":
                    E2BHydrationFlow()
                """
            )
        )

        result = subprocess.run(
            [sys.executable, str(flow_file), "--environment=conda", "run"],
            capture_output=True,
            text=True,
            timeout=1800,
        )

        assert result.returncode == 0, (
            f"Flow failed.\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        assert "E2B_E2E_OK helloWorld:6" in result.stdout
