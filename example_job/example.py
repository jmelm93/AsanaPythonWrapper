# Developers Doc - https://developers.asana.com/docs/python
# Docs for Asana Tasks API - https://developers.asana.com/reference/tasks

import logging
import os
import pandas as pd 
import datetime 
from classes.GoogleCloud import GoogleCloudClient
from classes.Asana import AsanaClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)


##### CONSTANTS #####

#date cols
date_cols_to_convert = ['modified_at', 'created_at', 'completed_at', 'due_on']

#get speciifc cols
cols = [
    'gid',
    'assignee_gid',
    'assignee_name', 
    'workspace_gid',
    'workspace_name',
    'team_name',
    'projects_gid',
    'projects_name',
    'name', 
    'notes', 
    'permalink_url', 
    'completed', 
    'modified_at', 
    'created_at',
    'completed_at',
    'due_on',
    'followers_gid',
    'followers_name',
    'memberships_section_gid',
    'memberships_section_name',
    'resource_type',
]

##### HELPER FUNCTIONS #####

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


def compare_dfs(df1, df2, col_unique_identifier, insert_timestamp):

    # Get a list of unique values for the unique identifier column from both dataframes
    unique_identifier_values = list(set(df1[col_unique_identifier].tolist() + df2[col_unique_identifier].tolist()))
    
    # create final cols
    newcols = ['change_status', 'last_change_seen']
    
    # convert df1.columns to column list and add new cols
    cols = df1.columns.tolist() + newcols
    
    # create an empty df to store the results
    result = pd.DataFrame(columns=cols)

    # Iterate through the unique identifier values
    for unique_id in unique_identifier_values:
        # check if unique_id is in df1
        if df1[col_unique_identifier].isin([unique_id]).any():
            # check if unique_id is in df2
            if df2[col_unique_identifier].isin([unique_id]).any():
                # check if modified_at is the same
                d1_value = df1[df1[col_unique_identifier] == unique_id]['modified_at'].values[0]
                d2_value = df2[df2[col_unique_identifier] == unique_id]['modified_at'].values[0]
                if d1_value == d2_value:
                    # If the values are equal, set the change status to "existing"
                    temp_df = df1[df1[col_unique_identifier] == unique_id]
                    temp_df.loc[:, 'change_status'] = 'existing'
                    result = pd.concat([result, temp_df], ignore_index=True)
                else:
                    # If the values are not equal, set the change status to "updated"
                    temp_df = df1[df1[col_unique_identifier] == unique_id]
                    temp_df.loc[:, 'change_status'] = 'updated'
                    temp_df.loc[:, 'last_change_seen'] = insert_timestamp
                    result = pd.concat([result, temp_df], ignore_index=True)
            else:
                # If the unique identifier (asana url) exists in df1 but not in df2, set the change status to "new"
                temp_df = df1[df1[col_unique_identifier] == unique_id]
                temp_df.loc[:, 'change_status'] = 'new'
                temp_df.loc[:, 'last_change_seen'] = insert_timestamp
                result = pd.concat([result, temp_df], ignore_index=True)
        else:
            # If the unique identifier (asana url) exists in df2 but not in df1, set the change status to "deleted"
            temp_df = df2[df2[col_unique_identifier] == unique_id]
            temp_df.loc[:, 'change_status'] = 'deleted'
            temp_df.loc[:, 'last_change_seen'] = insert_timestamp            
            result = pd.concat([result, temp_df], ignore_index=True)
    return result


def last_updated_in_n_weeks(df, last_modified_date):
    # check modified col and create a new col called "last_update_n_weeks_ago" and include 1, 2, 3, 4, or 4+ weeks ago
    df['last_update_n_weeks_ago'] = df[last_modified_date].apply(lambda x: (datetime.datetime.now() - x).days // 7)
    df['last_update_n_weeks_ago'] = df['last_update_n_weeks_ago'].apply(lambda x: '4+' if x > 4 else x)
    return df


def try_to_convert_to_formatted_date(x):
    try:
        return pd.to_datetime(x).strftime('%Y-%m-%d')
    except:
        return None 


##### CORE JOB #####

def main(workspace, output_dir, token):
    
    #timestamp for job
    insert_timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    #initialize the AsanaClient
    client = AsanaClient(token)
    
    #get the gid for the workspace
    gid_for_workspace = client.get_workspace_id_by_workspace_name(workspace)
    
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
    
    #filter task details df to cols
    df = task_details_df[cols]
    
    # #convert date cols to datetime
    for col in date_cols_to_convert:
        df[col] = df[col].apply(try_to_convert_to_formatted_date)
    
    # add "insert_timestamp" column
    df.assign(insert_timestamp=insert_timestamp)
    
    # get table_id
    table_id = f"{os.getenv('BIGQUERY_PROJECT_ID')}.{os.getenv('BIGQUERY_DATASET_ID')}.{os.getenv('BIGQUERY_TABLE_ID')}"
    
    #query current table if exists
    df_database = None
    
    try: 
        df_database = pd.read_gbq(f"SELECT * FROM `{table_id}`", project_id=os.getenv('BIGQUERY_PROJECT_ID'))
    except:
        pass
    
    #compare current table to new table
    if df_database is not None:
        df = compare_dfs(df, df_database, 'permalink_url', insert_timestamp)
    else:
        df.loc[:, 'change_status'] = 'new'
        df.loc[:, 'last_change_seen'] = insert_timestamp
    
    #drop duplicates based on permalink_url
    df = df.drop_duplicates(subset=['permalink_url'], keep='first')
    
    #create modified_at_datetime col then add last updated weeks ago
    df['modified_at_datetime'] = df['modified_at'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    df = last_updated_in_n_weeks(df, 'modified_at_datetime')
    
    #drop col modified_at_datetime
    df = df.drop(columns=['modified_at_datetime'])
    
    #create a dictionary to pass to the write_to_bigquery_tables method
    data = { "table_id": table_id, "data": df }

    #initialize the GoogleCloudClient
    gcc = GoogleCloudClient(service_account_path='service_accounts/sa.json', write_disposition='WRITE_TRUNCATE')

    #write the data to BigQuery
    gcc.write_to_bigquery_tables(data)


if __name__ == '__main__':
    ASANA_PERSONAL_ACCESS_TOKEN = os.getenv('ASANA_PERSONAL_ACCESS_TOKEN')
    main(workspace="3Q Digital", output_dir="export_data", token=ASANA_PERSONAL_ACCESS_TOKEN)