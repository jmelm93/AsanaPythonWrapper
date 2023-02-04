# Developers Doc - https://developers.asana.com/docs/python
# Python Asana Github - https://github.com/Asana/python-asana/

from AsanaClass import AsanaClient
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

PERSONAL_ACCESS_TOKEN = os.getenv('PERSONAL_ACCESS_TOKEN')

def main():
    client = AsanaClient(PERSONAL_ACCESS_TOKEN)
    gid_for_3q = client.get_3q_digital_workspace_id()
    if gid_for_3q is None:
        logging.error("3Q Digital Workspace not found")
        return
    project_list = client.list_projects(gid_for_3q)
    task_list = client.list_tasks_by_project(project_list[0]['gid'])
    list_task_details = [ client.get_task_details_by_gid(task['gid']) for task in task_list ]
    client.helper_write_list_of_objects_to_json(list_task_details, 'data/task_details.json')

if __name__ == '__main__':
    main()