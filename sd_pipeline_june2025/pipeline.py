import os.path
from datetime import timezone

from openhexa.sdk import current_run, pipeline, workspace, parameter
from datetime import datetime
import papermill as pm


@pipeline("with-papermill", timeout=720)  # 12 minutes (en secondes)
@parameter("user_name", name="User name", type=str, default="Stephane", help="Nom de l'utilisateur")
@parameter("politesse_lvl", name="Niveau de politesse", type=str, choices=['rude', 'respectueux','frotte manche'], default='respectueux', help="Choix du niveau de politesse")
@parameter("data_element_list", name="Data Element List", type=str, choices=["DE_list_1", "DE_list_2"], default="DE_list_1", help="Choix de la liste des Data Elements")




def with_papermill(user_name, politesse_lvl, data_element_list):
    current_run.log_info("Pipeline started.")
    run_notebook(user_name, politesse_lvl, data_element_list)


@with_papermill.task
def run_notebook(user_name, politesse_lvl, data_element_list):  

    # Génère le message de politesse selon le niveau choisi
    if politesse_lvl == "rude":
        message_de_politesse = "Pas trop tôt, enfin qqn test cette pipeline!"
    elif politesse_lvl == "respectueux":
        message_de_politesse = "Merci d'avoir pris le temps de tester cette pipeline."
    elif politesse_lvl == "frotte manche":
        message_de_politesse = "C'est vraiment génial que vous testiez cette pipeline, vous êtes vraiment exceptionnel !"
    else:
        message_de_politesse = "Merci."


    current_run.log_info("Launching the notebook...")
    
    input_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "notebook_extraction_dhis2.ipynb")
    current_run.log_debug(f"Input notebook path: {input_path}")
    
    output_path = f"{workspace.files_path}/simple_notebook_output_{datetime.now(timezone.utc).isoformat()}.ipynb"
    current_run.log_debug(f"Output notebook path: {output_path}")

    current_run.log_info(f"Les paramètres choisis sont : {user_name}, {politesse_lvl}, {data_element_list}")

    try:
        pm.execute_notebook(
            input_path=input_path,
            output_path=output_path,
            parameters={
                "user_name": user_name,
                "data_element_list": data_element_list,
                "politesse_lvl": politesse_lvl
                },
            request_save_on_cell_execute=False,
            progress_bar=False
        )
        current_run.log_info("Notebook executed successfully.")
    except Exception as e:
        current_run.log_error(f"Failed to execute notebook: {e}")
        raise
    


    # On ajoute aussi le CSV généré par le notebook comme output pipeline
    date_today = datetime.now().strftime("%Y-%m-%d")
    csv_filename = f"agg_df_{date_today}.csv"
    csv_output_path = os.path.join(workspace.files_path, csv_filename)

    current_run.log_debug(f"Looking for CSV output at: {csv_output_path}")
    
    if os.path.exists(csv_output_path):
        current_run.log_info(f"Adding pipeline output file: {csv_output_path}")
        current_run.add_file_output(csv_output_path)
    else:
        current_run.log_warning(f"CSV output file not found at expected path: {csv_output_path}")
    


    
    current_run.log_info("Task done!")
    current_run.log_info("Pipeline finished.")
    current_run.log_info(message_de_politesse)

if __name__ == "__main__":
    with_papermill()


# Commande pour pousser vers OpenHexa

### openhexa pipelines run .
### openhexa pipelines push .