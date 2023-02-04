# Asana Python Client Wrapper
A Python script for interfacing with the Asana API to retrieve task details for a specific project.

## Prerequisites
- Asana API Key (Personal Access Token)
- python3
- python libraries: asana, os, dotenv, logging, and json

## Setup
1. Clone this repository
2. Create a file named ".env" in the root directory of the repository.
3. Add the following line to the ".env" file, replacing "your-api-key" with your actual API key:
```
#.env file
PERSONAL_ACCESS_TOKEN=your-api-key
```
4. [Optional, but Recommended] Create a virtual environment for your project
```
python3 -m venv venv

# if on linux or mac OS - run the below to activate the venv
source venv/bin/activate

# if on windows OS - run the below to activate the venv
.\venv\Scripts\activate

```
5. Install the required libraries by running the following command:
```
pip install -r requirements.txt
```
6. Update the "workspace" found in the below line of code to match the desired workspace for the data extraction

```
if __name__ == '__main__':
    main(workspace="3Q Digital") # Update with your desired workspace
```

## Usage

The script retrieves all the tasks details for a specific project in the 3Q Digital Workspace and writes the details to a JSON file named "task_details.json" in the "data" directory.  

To run the script, execute the following command in your terminal:
```
python3 main.py
```

## Class: AsanaClient

This class provides an interface to the Asana API and contains the following methods:

- `__init__`: The constructor that initializes the client, retrieves the user's information, and retrieves the workspace ID list.
- `get_workspace_id_by_workspace_name`: Returns the ID for the workspace that matches the input "workspace_name".
- `get_projects`: Retrieves a list of projects in a workspace.
- `list_projects`: Wraps the get_projects method and returns a list of projects.
- `list_tasks_by_project`: Retrieves a list of tasks for a specific project.
- `get_task_details_by_gid`: Retrieves task details for a specific task.
- `helper_write_list_of_objects_to_json`: Writes a list of objects to a JSON file.    
- `helper_flatten_dict`: Flattens a nested dictionary to a single level.
- `helper_clean_task_data`: Cleans task data by extracting relevant information and removing unnecessary details.