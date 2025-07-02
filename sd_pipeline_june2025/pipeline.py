import os.path
from datetime import timezone

from openhexa.sdk import current_run, pipeline, workspace
from datetime import datetime
import papermill as pm


@pipeline("with-papermill")
def with_papermill():
    run_notebook()


@with_papermill.task
def run_notebook():
    current_run.log_info("Launching the notebook...")
    input_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "notebook_extraction_dhis2.ipynb")
    output_path = f"{workspace.files_path}/simple_notebook_output_{datetime.now(timezone.utc).isoformat()}.ipynb"
    pm.execute_notebook(
        input_path=input_path,
        output_path=output_path,
        parameters={"param_1": "value_1", "param_2": False},
        # The next parameter is important - otherwise papermill will perform a lot of small append write operations,
        # which can be very slow when using object storage in the cloud
        request_save_on_cell_execute=False,
        progress_bar=False
    )
    current_run.log_info("Done!")


if __name__ == "__main__":
    with_papermill()