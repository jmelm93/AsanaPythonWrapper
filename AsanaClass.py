import asana
import os
import json
from dotenv import load_dotenv

load_dotenv()

PERSONAL_ACCESS_TOKEN = os.getenv('PERSONAL_ACCESS_TOKEN')

class AsanaClient:
    def __init__(self, personal_access_token):
        self.client = asana.Client.access_token(personal_access_token)
        self.me = self.client.users.me()
        self.workspace_id_list = self.me['workspaces']
        
    def helper_write_list_of_objects_to_json(self, list_of_objects, file_name):
        with open(file_name, 'w') as f:
            json.dump(list_of_objects, f)

    def get_3q_digital_workspace_id(self):
        for workspace in self.workspace_id_list:
            if workspace['name'] == '3Q Digital':
                return workspace['gid']
        return None

    def get_projects(self, workspace_id):
        return self.client.projects.find_by_workspace(workspace_id, {'archived': False})

    def list_projects(self, workspace_id):
        projects = self.get_projects(workspace_id)
        return [project for project in projects]
    
    def list_tasks_by_project(self, project_id):
        tasks = self.client.tasks.find_by_project(project_id, {'archived': False})
        return [task for task in tasks]
    
    def get_task_details_by_gid(self, task_gid):
        return self.client.tasks.find_by_id(task_gid)



