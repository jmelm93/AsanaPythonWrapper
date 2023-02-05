# Developers Doc - https://developers.asana.com/docs/python
# Docs for Asana Tasks API - https://developers.asana.com/reference/tasks

from classes.Asana import AsanaClient
import logging
import os
import pandas as pd 
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)


def get_all_teams(client, workspace, output_dir=None, export=True):
    teams_object = client.get_teams(workspace)
    team_list = list(teams_object)
    logging.info(f"Number of teams: {len(team_list)}")
    if export:
        client.helper_write_list_of_objects_to_json(team_list, f'{output_dir}/teams.json')
    return team_list


def get_projects_for_team(client, team_list):
    for i, team in enumerate(team_list):
        logging.info(f"Getting Projects - Processed {i} teams out of {len(team_list)}")
        projects_object = client.get_projects_from_team(team['gid'])
        projects_list = list(projects_object)
        # if projects exist, then append team to each item and yield
        if len(projects_list) > 0:
            for project in projects_list:
                project['team_name'] = team['name']
                yield project     


def get_tasks_from_project_list(client, projects_list):
    for i, project in enumerate(projects_list):
        logging.info(f"Getting Tasks - Processed {i} projects out of {len(projects_list)}")
        tasks_object = client.list_tasks_by_project(project['gid'])
        tasks_list = list(tasks_object)
        if len(tasks_list) > 0:
            for task in tasks_list:
                # add the project and team name to the task
                task['project_name'] = project['name']
                task['team_name'] = project['team_name']
                yield task


def get_task_details_from_task_list(client, task_list):
    for i, task in enumerate(task_list):
        logging.info(f"Task Details - Processed {i} tasks out of {len(task_list)}")
        task_detail = client.get_task_details_by_gid(task['gid'])
        task_detail['project_name'] = task['project_name']
        task_detail['team_name'] = task['team_name']
        yield task_detail


def main(workspace, output_dir, token):
    
    client = AsanaClient(token)
    
    gid_for_workspace = client.get_workspace_id_by_workspace_name(workspace)
    
    if gid_for_workspace is None:
        logging.error(f"Workspace with the name {workspace} does not exist.")
        return
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    #get all teams in org
    team_list = get_all_teams(client, gid_for_workspace, output_dir)
    
    #get all projects for each team
    project_list_generator = get_projects_for_team(client, team_list)
    project_list = list(project_list_generator)
    
    #get all tasks for each project
    task_list_generator = get_tasks_from_project_list(client, project_list)
    task_list = list(task_list_generator)

    #get all task details for each task
    task_detail_list_generator = get_task_details_from_task_list(client, task_list)
    task_detail_list = list(task_detail_list_generator)
    
    #map the helper function to each task detail
    task_details_df = pd.DataFrame(map(client.helper_clean_task_data, task_detail_list))
    
    #write task details to csv in output_dir
    task_details_df.to_csv(f'{output_dir}/task_details_test2.csv', index=False)
    
if __name__ == '__main__':
    ASANA_PERSONAL_ACCESS_TOKEN = os.getenv('ASANA_PERSONAL_ACCESS_TOKEN')
    main(workspace="3Q Digital", output_dir="export_data", token=ASANA_PERSONAL_ACCESS_TOKEN)