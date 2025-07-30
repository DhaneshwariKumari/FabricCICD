# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##### Post Deployment Activity
# 
# After cloning a workspace or importing items into a workspace, this notebook will reconfigure any references to the old workspace by rebinding them to the new workspace. 
# 
# For example a pipeline referencing a warehouse or a default lakehouse of a notebook.
# 
# Summary of post activities in order:
# <ul>
# <li>Default lakehouses and warehouse are updated to local lakehouse/warehouses</li>
# <li>Either creates shortcuts in local lakehouse back to tables in the source lakehouse, or copies the data from source lakehouse. Set via parameter below.</li>
# <li>Copy warehouse data. Set via parameter below</li>
# <li>Changes directlake semantic model connections for semantic models to "local" lakehouse/warehouse</li> 
# <li>Rebinds reports to "local" semantic models</li>
# <li>Changes pipeline lakehouse/warehouse references to local item</li>
# <li>Ability to swap connections in pipelines from old to new</li>
# <li>Commit changes to git</li>
# </ul>
# 
# Requirements:
# <ul>
# <li>Requires Semantic Link Labs installed by pip install below or added to environment library.</li>
# <li>Requires JmesPath library for data pipeline JSON manipulation i.e. connection swaps.</li>
# </ul>
# 
# Limitations of current script:
# 
# <ul>
# <li>Does not recreate item shares or external shortcuts</li>
# <li>Does not re-apply lakehouse SQL Endpoint or Warehouse object/row/column level security</li>
# <li>Does not recreate data access roles in Lakehouse</li>
# <li>Untested with Lakehouses where with schema support enabled</li>
# </ul>
# 
# 


# MARKDOWN ********************

# ##### Install semantic link labs
# 
# Required to support advanced functionality 
# 
# https://semantic-link-labs.readthedocs.io/en/latest/index.html<br>
# https://github.com/microsoft/semantic-link-labs/blob/main/README.md
# 


# CELL ********************

!pip -q install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Install Jmespath
# 
# Required for data pipeline changes such as updating linked notebooks, warehouses and lakehouses 

# CELL ********************

!pip install jmespath

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ##### Set these parameters if running as a standaone noteook
# Before running this notebook ensure these parameters are set correctly.

# PARAMETERS CELL ********************

# specify the target workspaces to update
target_ws = 'FabricCICDVBD_Feature'

# the target lakehouse to reconnect the direct lake models to use
target_lakehouse='bronze'

# Set connections to be replaced from previous name or ID to new name or ID.
connections_from_to = () #('https://api.fabric.microsoft.com/v1/workspaces/ admin','4498340c-27cf-4c6e-a025-00e5de6b0726'),('4498340c-27cf-4c6e-a025-00e5de6b0726','https://api.fabric.microsoft.com/v1/workspaces/ admin'),('https://api.fabric.microsoft.com/v1/workspaces/ admin','4498340c-27cf-4c6e-a025-00e5de6b0726')

### Do not change these parameters  ####
# internal parameter to allow the installation of Python libraries when being run programatically. See https://learn.microsoft.com/en-us/fabric/data-engineering/library-management#python-inline-installation
_inlineInstallationEnabled = True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Library imports and fabric rest client setup
# 
# https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric.fabricrestclient

# CELL ********************

import pandas as pd
import datetime, time
import re,json, fnmatch,os
import requests, base64,ast
import sempy
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,current_timestamp,lit
import sempy_labs as labs
from sempy_labs import migration, directlake
from sempy_labs import lakehouse as lake
from sempy_labs import report as rep
from sempy_labs.tom import connect_semantic_model
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
    _decode_b64,
    _base_api,
)
import sempy_labs._icons as icons
from jsonpath_ng import jsonpath, parse
from typing import Optional
from typing import Optional, Tuple, List
from uuid import UUID


# instantiate the Fabric rest client
def get_token(audience="pbi"):
    return notebookutils.credentials.getToken(audience)
client = fabric.FabricRestClient(token_provider=get_token)

# get the current workspace ID based on the context of where this notebook is run from
thisWsId = notebookutils.runtime.context['currentWorkspaceId']
thisWsName = notebookutils.runtime.context['currentWorkspaceName']

target_ws_id = fabric.resolve_workspace_id(target_ws)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Always run this cell
# 
# Contains utility functions

# CELL ********************

#### 
### Utility functions 
####

def _is_valid_uuid(
    guid: str,
):
    """
    Validates if a string is a valid GUID in version 4

    Parameters
    ----------
    guid : str
        GUID to be validated.

    Returns
    -------
    bool
        Boolean that indicates if the string is a GUID or not.
    """

    try:
        UUID(str(guid), version=4)
        return True
    except ValueError:
        return False

def get_capacity_status(p_target_cap):
    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Id"] == p_target_cap]
    return dfC_filt['State'].iloc[0]


def getItemId(wks_id,itm_name,itm_type):
    df = fabric.list_items(type=None,workspace=wks_id)
    #print(df)
    if df.empty:
        return 'NotExists'
    else:
        #display(df)
        #print(df.query('"Display Name"="'+itm_name+'"'))
        if itm_type != '':
            newdf= df.loc[(df['Display Name'] == itm_name) & (df['Type'] == itm_type)]['Id']
        else:
            newdf= df.loc[(df['Display Name'] == itm_name)]['Id']  
        if newdf.empty:
            return 'NotExists'
        else:
            return newdf.iloc[0]


########
### Pipeline utilities
########

def update_data_pipeline_definition(
    name: str, pipeline_content: dict, workspace: Optional[str] = None
):
    """
    Updates an existing data pipeline with a new definition.

    Parameters
    ----------
    name : str
        The name of the data pipeline.
    pipeline_content : dict
        The data pipeline content (not in Base64 format).
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.FabricRestClient()
    pipeline_payload = base64.b64encode(json.dumps(pipeline_content).encode('utf-8')).decode('utf-8')
    pipeline_id = fabric.resolve_item_id(
        item_name=name, type="DataPipeline", workspace=workspace
    )

    request_body = {
        "definition": {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": pipeline_payload,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }


    response = client.post(
        f"v1/workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition",
        json=request_body,
    )

    lro(client, response, return_status_code=True)

    print(
        f"{icons.green_dot} The '{name}' pipeline was updated within the '{workspace}' workspace."
    )

# Swaps the connection properties of an activity belonging to the specified item type(s)
def swap_pipeline_connection(pl_json: dict, p_target_ws: str, 
                                p_item_type: List =['DataWarehouse','Lakehouse','Notebook'], 
                                p_conn_from_to: Optional[List[Tuple[str,str]]]=[]):
    
    target_ws_id = fabric.resolve_workspace_id(target_ws)

    if 'Warehouse' in p_item_type or 'Lakehouse' in p_item_type:
        ls_expr = parse('$..linkedService')
        for endpoint_match in ls_expr.find(pl_json):
            if endpoint_match.value['properties']['type'] == 'DataWarehouse' \
                and endpoint_match.value['properties']['typeProperties']['workspaceId'] != target_ws_id \
                and 'Warehouse' in p_item_type:
                # only update the warehouse if it was located in the source workspace i.e. we will update the properties to the target workspace if the warehouse resided in the same workspace as the pipeline
                warehouse_id = endpoint_match.value['properties']['typeProperties']['artifactId']
                warehouse_endpoint = endpoint_match.value['properties']['typeProperties']['endpoint']
                
                source_wh_name = fabric.resolve_item_name(item_id = warehouse_id,workspace=target_ws_id)
                # find the warehouse id of the warehouse with the same name in the target workspace
                target_wh_id = fabric.resolve_item_id(item_name = source_wh_name,type='Warehouse',workspace=target_ws_id)
                # look up the connection string for the warehouse in the target workspace
                whurl  = f"v1/workspaces/{target_ws_id}/warehouses/{target_wh_id}"
                whresponse = client.get(whurl)
                lhconnStr = whresponse.json()['properties']['connectionString']
                endpoint_match.value['properties']['typeProperties']['artifactId'] = target_wh_id
                endpoint_match.value['properties']['typeProperties']['workspaceId'] = target_ws_id
                endpoint_match.value['properties']['typeProperties']['endpoint'] = lhconnStr
                ls_expr.update(endpoint_match,endpoint_match.value)
            if endpoint_match.value['properties']['type'] == 'Lakehouse' \
                and endpoint_match.value['properties']['typeProperties']['workspaceId'] != target_ws_id \
                and 'Lakehouse' in p_item_type:
                #print(endpoint_match.value)
                lakehouse_id = endpoint_match.value['properties']['typeProperties']['artifactId']
                remote_lh_name = fabric.resolve_item_name(item_id = lakehouse_id,workspace=target_ws_id)
                # find the lakehouse id of the lakehouse with the same name in the target workspace
                target_lh_id = fabric.resolve_item_id(item_name = remote_lh_name,type='Lakehouse',workspace=target_ws_id)
                endpoint_match.value['properties']['typeProperties']['artifactId'] = target_lh_id
                endpoint_match.value['properties']['typeProperties']['workspaceId'] = target_ws_id
                ls_expr.update(endpoint_match,endpoint_match.value)
                #    print(endpoint_match.value)



    if 'Notebook' in p_item_type: 
        ls_expr = parse('$..activities')

        for endpoint_match in ls_expr.find(pl_json):
            for activity in endpoint_match.value:
                #print(activity['type'])
                if activity['type']=='TridentNotebook' and 'Notebook' in p_item_type: #only update if the notebook was in the same workspace as the pipeline
                    print('change from '+activity['typeProperties']['workspaceId'])
                    source_nb_id = activity['typeProperties']['notebookId']
                    source_nb_name = fabric.resolve_item_name(item_id = source_nb_id,workspace=source_ws_id)
                    target_nb_id = fabric.resolve_item_id(item_name = source_nb_name,type='Notebook',workspace=target_ws_id)
                    activity['typeProperties']['notebookId']=target_nb_id
                    activity['typeProperties']['workspaceId']=target_ws_id
                    print('to notebook '+ target_nb_id)
                    #ls_expr.update(endpoint_match,endpoint_match.value)

    if len(p_conn_from_to)>0 and len(p_conn_from_to[0])>0 :
        for ti_conn_from_to in p_conn_from_to:
            if ti_conn_from_to[0] and len(ti_conn_from_to[0])>0:
                if not _is_valid_uuid(ti_conn_from_to[0]):
                    print('Connection from is string '+ str(ti_conn_from_to[0]))
                    dfC_filt = df_conns[df_conns["Connection Name"] == ti_conn_from_to[0]]       
                    connId_from = dfC_filt['Connection Id'].iloc[0]     
                else:
                    connId_from = ti_conn_from_to[0]

                if not _is_valid_uuid(ti_conn_from_to[1]):
                    print('Connection from is string '+ str(ti_conn_from_to[1]))
                    dfC_filt = df_conns[df_conns["Connection Name"] == ti_conn_from_to[1]]       
                    connId_to = dfC_filt['Connection Id'].iloc[0]     
                else:
                    connId_to = ti_conn_from_to[1]

                ls_expr = parse('$..externalReferences')
                for externalRef in ls_expr.find(pl_json):
                    if externalRef.value['connection']==connId_from:
                        print('Changing connection from '+str(connId_from))
                        externalRef.value['connection']=connId_to
                        ls_expr.update(externalRef,externalRef.value)
                        print('to '+str(connId_to))

    return pl_json



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Run pipeline to create the lakehouse data/table
# 
# This is required before semantic model is rebound and refreshed otherwise the latter will fail because the table does not exist

# CELL ********************

pls = labs.list_data_pipelines(target_ws)
for p,i in pls.iterrows():
    try:
        plid = i['Data Pipeline ID']
        plurl = f'v1/workspaces/{target_ws_id}/items/{plid}/jobs/instances?jobType=Pipeline'
        #print(plurl)

        payload_data = '{}'
        plresponse = client.post(plurl, json=json.loads(payload_data))

        if plresponse.status_code==202:
            print(f"Running {i['Data Pipeline Name']} pipeline, please wait...")
            location_url = plresponse.headers.get("Location")
            retry_after = int(plresponse.headers.get("Retry-After", 5))  # Default to 5 seconds if not provided

            #print(f"Job with the location: '{location_url}' has been triggered  with a status check of '{retry_after}' seconds.")

            # Polling for operation status
            while True:
                time.sleep(10) # there is a delay between having the status updated after calling the job instance api so adding a 10 seconds wait
                operation_status_response = client.get(f"{location_url}")
                operation_state = operation_status_response.json()
                #print(operation_state)

                status = operation_state.get("status")
                print(f"Operation status: {status}")

                if status in ["NotStarted", "Running"]:
                    print(f"The job is still running or is not started")
                    time.sleep(retry_after)
                else:
                    break

            # Final check on operation status
            if status == "Failed":
                error_response = operation_state.get('failureReason', {}).get('message', '')
                print(f"{icons.red_dot} The pipeline failed. Error response: {error_response}")
                raise ValueError(f"The pipeline failed. Please review the monitoring snapshot in Fabric for more detail. Error response: {error_response}")

            else:
                print(f"{icons.green_dot} {i['Data Pipeline Name']}  pipeline has complete successfully.")  

        else:
            print('An error occurred when trying to invoke job: ' + str(plresponse.status_code) + ' - ' + plresponse.text)
            raise ValueError("Error invoking ingestion pipeline. Please review the debug logs and correct before continuing with this notebook.")
    except FabricHTTPException as e:
        print('Caught a FabricHTTPException. Check the API endpoint, authentication.', e)   


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Update default and attached lakehouses/warehouses for notebooks
# 
# Update notebook dependencies based on but now supports T-SQL notebooks:
# https://github.com/PowerBiDevCamp/FabConWorkshopSweden/blob/main/DemoFiles/GitUpdateWorkspace/updateWorkspaceDependencies_v1.ipynb


# CELL ********************

for notebook in notebookutils.notebook.list(workspaceId=target_ws_id):
    updates = False
    if True: #notebook.displayName == 'T-SQL_Notebook': #notebook.displayName != 'Create Feature Branch':

        # Get the current notebook definition
        json_payload = json.loads(notebookutils.notebook.getDefinition(notebook.displayName,workspaceId=target_ws_id))
        #print(json.dumps(json_payload, indent=4))
        # Check for any attached lakehouses
        if 'dependencies' in json_payload['metadata'] \
            and 'lakehouse' in json_payload['metadata']['dependencies'] \
            and json_payload['metadata']["dependencies"]["lakehouse"] is not None:
            # Extract attached and default lakehouses
            current_lakehouse = json_payload['metadata']['dependencies']['lakehouse']
            # if default lakehouse setting exists and it is part of the target workspace then updated it to lakehouse in the target workspace 
            if 'default_lakehouse_name' in current_lakehouse and  json_payload['metadata']['dependencies']['lakehouse']['default_lakehouse_workspace_id'] != target_ws_id:
                print(f"Updating notebook {notebook.displayName} with new default lakehouse: {current_lakehouse['default_lakehouse_name']} in workspace {target_ws}")
                current_lakehouse['default_lakehouse'] = fabric.resolve_item_id(item_name = json_payload['metadata']['dependencies']['lakehouse']['default_lakehouse_name'],type='Lakehouse',workspace=target_ws_id)
                current_lakehouse['default_lakehouse_workspace_id'] = target_ws_id
                updates = True
            # loop through all attached lakehouess
            for lakehouse in json_payload['metadata']['dependencies']['lakehouse']['known_lakehouses']:
                # find target lakehouse id based on name - in this case we are assuming it is the one lakehouse configured in the demo workspace
                target_lh_id = fabric.resolve_item_id(item_name = current_lakehouse['default_lakehouse_name'],type='Lakehouse',workspace=target_ws_id)
                for known_lakehouses in json_payload['metadata']['dependencies']['lakehouse']['known_lakehouses']:
                    if known_lakehouses['id']!=target_lh_id:
                        known_lakehouses['id'] = target_lh_id
                        print(f"Updating known lakehouse {current_lakehouse['default_lakehouse_name']} to target ID {target_lh_id}")
                        updates = True

        if 'dependencies' in json_payload['metadata'] and 'warehouse' in json_payload['metadata']['dependencies']:
            # Fetch existing details
            current_warehouse = json_payload['metadata']['dependencies']['warehouse']
            current_warehouse_id = current_warehouse['default_warehouse']
            source_wh_name =  fabric.resolve_item_name(item_id = current_warehouse_id,workspace=target_ws_id)
            #print('Source warehouse name is ' + source_wh_name)
            target_wh_id = fabric.resolve_item_id(item_name = source_wh_name,type='Warehouse',workspace=target_ws_id)

            if 'default_warehouse' in current_warehouse:
                #json_payload['metadata']['dependencies']['warehouse'] = {}
                print(f"Attempting to update notebook {notebook.displayName} with new default warehouse: {target_wh_id} in {target_ws}")
            
                json_payload['metadata']['dependencies']['warehouse']['default_warehouse'] = target_wh_id
                for warehouse in json_payload['metadata']['dependencies']['warehouse']['known_warehouses']:
                    if warehouse['id'] == current_warehouse_id:
                        warehouse['id'] = target_wh_id
                        updates = True

        if updates:
            notebookutils.notebook.updateDefinition(
                    name = notebook.displayName,
                    content  = json.dumps(json_payload),
                    workspaceId = target_ws_id
                    )
            
            print(f"Updated notebook {notebook.displayName} in {target_ws}")

        else:
            print(f'No updates required for notebook {notebook.displayName}, ignoring.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ##### Update directlake model lakehouse/warehouse connection
# 
# https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_model_connection    

# CELL ********************

time.sleep(60) # waiting for sql endpoint to process
df_datasets = fabric.list_datasets(target_ws)

# Iterate over each dataset in the dataframe
for index, row in df_datasets.iterrows():
    try:
        # Check if the dataset is not the default semantic model
        if not labs.is_default_semantic_model(row['Dataset Name'], fabric.resolve_workspace_id(target_ws)):
            #print('Updating semantic model connection ' + row['Dataset Name'] + ' in workspace '+ target_ws)
            labs.directlake.update_direct_lake_model_connection(dataset=row['Dataset Name'], 
                                                                            workspace= target_ws,
                                                                            source='bronze', 
                                                                            source_type='Lakehouse', 
                                                                            source_workspace=target_ws)
            labs.refresh_semantic_model(dataset=row['Dataset Name'], workspace= target_ws)
    except Exception as error:
        errmsg =  f"Failed to update and refresh semantic model {row['Dataset Name']} due to: {str(error)}"
        print(errmsg)
        #raise ValueError(errmsg)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Rebind reports to local datasets
# 
# https://semantic-link-labs.readthedocs.io/en/latest/sempy_labs.report.html#sempy_labs.report.report_rebind

# CELL ********************

df_reports = fabric.list_reports(workspace=target_ws)
for index, row in df_reports.iterrows():
    #print(row['Name'] + '-' + row['Dataset Id'])
    df_datasets = fabric.list_datasets(workspace=target_ws)
    dataset_name = df_datasets[df_datasets['Dataset ID'] == row['Dataset Id']]['Dataset Name'].values[0]
    print(f'Rebinding report to {dataset_name} in {target_ws}')
    labs.report.report_rebind(report=row['Name'],dataset=dataset_name, report_workspace=target_ws, dataset_workspace=target_ws)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ##### Update data pipeline source & sink connections
# 
# Support changes lakehouses, warehouses, notebooks and connections from source to target. <br>
# Connections changes should be expressed as an array of tuples [{from_1:to_1},{from_N:to_N}]

# CELL ********************

# convert from a string to a proper type i.e. list of tuples 
# connections_from_to = ast.literal_eval(connections_from_to)
# loading a dataframe of connections to perform an ID lookup if required 
df_conns = labs.list_connections()

df_pipeline = labs.list_data_pipelines(target_ws)
for index, row in df_pipeline.iterrows():
    pipeline_json = json.loads(labs.get_data_pipeline_definition(row['Data Pipeline Name'],target_ws))

    p_new_json = swap_pipeline_connection(pipeline_json, target_ws,
            ['DataWarehouse','Lakehouse','Notebook'],
            [connections_from_to]) 
    #print(json.dumps(pipeline_json, indent=4))
    
    update_data_pipeline_definition(name=row['Data Pipeline Name'],pipeline_content=pipeline_json, workspace=target_ws)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ##### Commit changes made above to Git

# CELL ********************

if (p_option == 1 or (p_option in (2,3) and p_branch == 'dev')): # do not connect workspaces in options 2 & 3 if not dev branch
    labs.commit_to_git(comment='Initial',  workspace=target_ws)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
