# Developers Doc - https://developers.asana.com/docs/python
# Python Asana Github - https://github.com/Asana/python-asana/

from AsanaClass import AsanaClient
import logging
import os
import pandas as pd 
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

def main(workspace, output_dir, token):
    
    client = AsanaClient(token)
    
    gid_for_workspace = client.get_workspace_id_by_workspace_name(workspace)
    
    if gid_for_workspace is None:
        logging.error(f"Workspace with the name {workspace} does not exist.")
        return
    
    project_list = client.list_projects(gid_for_workspace)
    
    task_list = client.list_tasks_by_project(project_list[0]['gid'])
    
    list_task_details = [ client.get_task_details_by_gid(task['gid']) for task in task_list ]
    
    task_details_df = pd.DataFrame(map(client.helper_clean_task_data, list_task_details))
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    task_details_df.to_csv(f'{output_dir}/task_details.csv', index=False)

if __name__ == '__main__':
    PERSONAL_ACCESS_TOKEN = os.getenv('PERSONAL_ACCESS_TOKEN')
    main(workspace="3Q Digital", output_dir="export_data", token=PERSONAL_ACCESS_TOKEN)