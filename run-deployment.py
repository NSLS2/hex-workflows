from __future__ import annotations

from prefect.deployments import run_deployment

run_deployment(
    name="end-of-run-workflow/hex-end-of-run-workflow-docker",
    parameters={"stop_doc": {"run_start": ""}},
    timeout=15,  # don't wait for the run to finish # edit to 15 sec
)
