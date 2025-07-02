import os.path
from datetime import timezone

from openhexa.sdk import current_run, pipeline, workspace
from datetime import datetime
import papermill as pm


@pipeline("with-papermill", timeout=720)  # 12 minutes (en secondes)
def with_papermill():
    current_run.log_info("Pipeline started.")
    run_notebook()
    current_run.log_info("Pipeline finished.")


@with_papermill.task
def run_notebook():
    current_run.log_info("Launching the notebook...")
    
    input_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "notebook_extraction_dhis2.ipynb")
    current_run.log_debug(f"Input notebook path: {input_path}")
    
    output_path = f"{workspace.files_path}/simple_notebook_output_{datetime.now(timezone.utc).isoformat()}.ipynb"
    current_run.log_debug(f"Output notebook path: {output_path}")
    
    try:
        pm.execute_notebook(
            input_path=input_path,
            output_path=output_path,
            parameters={"param_1": "value_1", "param_2": False},
            # The next parameter is important - otherwise papermill will perform a lot of small append write operations,
            # which can be very slow when using object storage in the cloud
            request_save_on_cell_execute=False,
            progress_bar=False
        )
        current_run.log_info("Notebook executed successfully.")
    except Exception as e:
        current_run.log_error(f"Failed to execute notebook: {e}")
        raise
    
    current_run.add_file_output(output_path)
    current_run.log_info(f"Added notebook output file to pipeline outputs: {output_path}")
    
    # On ajoute aussi le CSV généré par le notebook comme output pipeline
    csv_output_path = os.path.join(workspace.files_path, "agg_df.csv")
    current_run.log_debug(f"Looking for CSV output at: {csv_output_path}")
    
    if os.path.exists(csv_output_path):
        current_run.log_info(f"Adding pipeline output file: {csv_output_path}")
        current_run.add_file_output(csv_output_path)
    else:
        current_run.log_warning(f"CSV output file not found at expected path: {csv_output_path}")
    
    current_run.log_info("Task done!")


if __name__ == "__main__":
    with_papermill()