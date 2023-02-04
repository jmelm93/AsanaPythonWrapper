import asana
import os
import json
from dotenv import load_dotenv

load_dotenv()

PERSONAL_ACCESS_TOKEN = os.getenv('PERSONAL_ACCESS_TOKEN')

class AsanaClient:
    def __init__(self, personal_access_token):
        """
        Initialize an Asana client with the provided personal access token.
        """
        self.client = asana.Client.access_token(personal_access_token)
        self.me = self.client.users.me()
        self.workspace_id_list = self.me['workspaces']


    def get_workspace_id_by_workspace_name(self, workspace_name):
        """
        Get the ID of the "3Q Digital" workspace.
        """
        for workspace in self.workspace_id_list:
            if workspace['name'] == workspace_name:
                return workspace['gid']
        return None


    def get_projects(self, workspace_id):
        """
        Get the projects within a given workspace.
        """
        return self.client.projects.find_by_workspace(workspace_id, {'archived': False})


    def list_projects(self, workspace_id):
        """
        List the projects within a given workspace.
        """
        projects = self.get_projects(workspace_id)
        return [project for project in projects]


    def list_tasks_by_project(self, project_id):
        """
        List the tasks within a given project.
        """
        tasks = self.client.tasks.find_by_project(project_id, {'archived': False})
        return [task for task in tasks]


    def get_task_details_by_gid(self, task_gid):
        """
        Get the details of a task with a given task ID.
        """
        return self.client.tasks.find_by_id(task_gid)


    def helper_write_list_of_objects_to_json(self, list_of_objects, file_name):
        """
        Writes a list of objects to a file in JSON format.
        """
        with open(file_name, 'w') as f:
            json.dump(list_of_objects, f)


    def helper_flatten_dict(self, d, parent_key='', sep='_'):
        """
            Used to flatten nested data (both nested dicts and nested lists of dicts)
        """
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                items.extend(self.helper_flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if all(isinstance(i, dict) for i in v):
                    for i in v:
                        items.extend(self.helper_flatten_dict(i, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            else:
                items.append((new_key, v))
        return dict(items)


    def helper_clean_task_data(self, task):
        """
            Clean the task data by flattening the nested data.
        """
        task_flattened = self.helper_flatten_dict(task)
        return task_flattened




