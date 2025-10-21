import json
from typing import Dict, List, Any
import sys
import os
import requests
import get_config as gc
import hsv_vault_connect as hconnect
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from tabulate import tabulate
import send_alert_email as email
import argparse
from pykeepass import PyKeePass


def parse_args():
     # define, parse and validate input arguments
    parser = argparse.ArgumentParser(
        description="Snowflake Terraform Drift Detection tool"
    )

    parser.add_argument(
        "alerts_location",
        type=str,
        metavar="",
        help="Alerts Location path",
    )

    parser.add_argument(
        "--tfc_api_base_url",
        type=str,
        default="https://app.terraform.io/api/v2",
        metavar="",
        help="Terraform Cloud API base URL",
    )

    parser.add_argument(
        "--tfc_org",
        type=str,
        default="tfdfagds",
        metavar="",
        help="Org Name in Terraform Cloud"
    )
    
    parser.add_argument(
        "--keepass_db",
        type=str,
        default="\\\\smof-fs01p\\techapps\\TECHNOLOGY\\PS_KP\\GDS\\GDS_SecretVault.kdbx",
        metavar="",
        help="KeePass database (.kdbx) path",
    )
    
    parser.add_argument(
        "--kp_key_file",
        type=str,
        default="\\\\dimensional.com\\dfaapps$\\Technology\\DataServices\\DS_EncryptionKeys\\Secure\GDS_KeyFile.key",
        metavar="",
        help="KeePass Key File",
    )

    parser.add_argument(
        "--tf_token_name",
        type=str,
        default="Terraform Team Token",
        metavar="",
        help="Terraform Token Entry name in Keypass",
    )


    parser.add_argument(
        "--accounts_config_file",
        type=str,
        default="config\config.json",
        metavar="",
        help="Snowflake Account Config File Location"
    )

    parser.add_argument(
        "--server_cert_path",
        type=str,
        default="cert\DFA-internal-CA-II.crt",
        metavar="",
        help= "Vault Server Certificate Location"
    )

    parser.add_argument(
        "--drift_config",
        type=str,
        default="config\drift_resource_attributes.json",
        metavar="",
        help="Drift Detection Config Location"
    )

    parser.add_argument(
        "--synonyms_config",
        type=str,
        default="config\synonyms.json",
        metavar="",
        help="Snowflake Resource Synonyms Location"
    )

    args = parser.parse_args()

    return args
    
    
def get_keepass_title_cred(kp_db:str,kp_key_file:str,kp_title:str):
    """
    Get credentials for given title from keepass

    Args:
        kp_db(str): keepass db
        kp_key_file(str): rsa pvt key file to login into keepass db
        kp_title(str): title to get credentials for
    Returns:
        object wiht title credentails
    """
    try:
        kp = PyKeePass(filename=kp_db, keyfile=kp_key_file)
        kp_cred = kp.find_entries(title=kp_title, first=True)
        return kp_cred
    except Exception as error:
        print("Error [get_keepass_title_cred] {}:{}".format(kp_title, error))


def load_resource_config(resource_config: str) -> List[Dict[str, Any]]:
    """
    Read the resource attributes configuration

    Args:
        resource_config(str): resource attributes file location
    Returns:
        List of dictionary values
    
    """
    try:
        with open(resource_config, "r") as f:
            return json.load(f)
    except Exception as e:
        raise RuntimeError(f"Error reading resource config file: {str(e)}")



def get_config(path):
    #Reading configuration information from the the config file
    try:
        with open(path) as config_file:
            config = json.load(config_file)
    except Exception as e:
        subject = 'Error Reading Snowflake Config'
        message = f"SNOWFLAKE AVAILABILITY ALERT: Failed to load config information {e}"
        sender_email = 'IT_GDS_Notification@Dimensional.com'
        recepient = "suresh.methuku@dimensional.com"
        email.send_email(subject, message, sender_email, recepient)
        exit
    return config

def load_synonyms(synonyms_config) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Load the synonyms mapping from synonyms.json
    """
    try:
        with open(synonyms_config, "r") as f:
            return json.load(f)
    except Exception as e:
        raise RuntimeError(f"Error reading synonyms config file: {str(e)}")

def get_workspace_id(org_name: str, workspace_name: str, tfc_api_base_url: str, headers: Dict[str, Any]):
    """Retrieves the workspace ID from its name."""
    url = f"{tfc_api_base_url}/organizations/{org_name}/workspaces"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    workspaces = response.json()["data"]
    for ws in workspaces:
        if ws["attributes"]["name"] == workspace_name:
            return ws["id"]
    raise ValueError(f"Workspace '{workspace_name}' not found in organization '{org_name}'")

def get_current_state_download_url(workspace_id: str, tfc_api_base_url: str, headers: Dict[str, Any] ):
    """Retrieves the download URL for the current state file."""
    url = f"{tfc_api_base_url}/workspaces/{workspace_id}/current-state-version"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["data"]["attributes"]["hosted-state-download-url"]

def download_state_file(download_url, tf_statefilename,  headers: Dict[str, Any] ):
    """Downloads the state file from the given URL."""
    response = requests.get(download_url, headers=headers)
    response.raise_for_status()
    with open(tf_statefilename, "wb") as f:
        f.write(response.content)
        
    with open(tf_statefilename, "r") as f:
        return json.load(f)
    
def get_terraform_state(tf_state_local: str, tfc_org: str, tfc_workspace_name: str, tfc_api_base_url: str, headers: Dict[str, Any] ):        
    try:
        workspace_id = get_workspace_id(tfc_org, tfc_workspace_name, tfc_api_base_url, headers)
        download_url = get_current_state_download_url(workspace_id, tfc_api_base_url, headers)
        return download_state_file(download_url,tf_state_local, headers)
    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
    except ValueError as e:
        print(f"Configuration Error: {e}")


def get_snowflake_resources(resource: str, attributes: List[str], sql_query: str, host: str, account_name: str, user_name: str, private_key: str, snow_warehouse: str, snow_role: str, snow_db: str) -> List[Dict[str, Any]]:
    #Load the private key.
    p_key= serialization.load_pem_private_key(
    private_key.encode(),
    password=None,
    backend=default_backend()
    )

    #Serialize the private key to DER format
    pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

    try:
        conn = snowflake.connector.connect(
            host = host,
            user = user_name,
            private_key = pkb,
            account = account_name,
            warehouse = snow_warehouse,
            role = snow_role,
            database = snow_db,
            session_parameters = {'MULTI_STATEMENT_COUNT': '0'}
            
            )
        cursor = conn.cursor()
        
        # Handle SHOW commands
        if sql_query.strip().upper().startswith("SHOW"):        
            cursor.execute(sql_query)     
            while cursor.nextset():
                rows = cursor.fetchall()
        else:
            cursor.excute(sql_query)
            rows = cursor.fetchall()
        

        #Get Column names
        columns = [col[0].upper() for col in cursor.description]
        col_index = {col: idx for idx, col in enumerate(columns)}

        for attr in attributes:
            if attr.upper() not in col_index:
                raise RuntimeError (f" Attribute '{attr}' not found in Snowflake query results for {resource}.")
        
        resources = []
        for row in rows:
            resource_dict = {}
            for attr in attributes:
                value = row[col_index[attr.upper()]]
                resource_dict[attr] = value
            resources.append(resource_dict)
        
        cursor.close()
        conn.close()
        return resources
        
    except Exception as e:
        raise RuntimeError(f"Failed to query Snowflake {resource}: {str(e)}")

def compare_resources(tf_state: Dict[str, Any], sf_resources: List[Dict[str, Any]], resource: str, attributes: List[str], synonyms: Dict[str, Dict[str, Dict[str, str]]]) -> List[Dict[str, Any]]:
    """
    Compare Terraform state with Snowflake resources for the specified resource type and attributes.
    """
    drifts = []
    tf_resource_type = f"snowflake_{resource.lower()}"

    # Extract resources from Terraform state
    tf_resources = []
    for res in tf_state.get("resources", []):
        if res["type"] == tf_resource_type:
            tf_resources.extend([instance["attributes"] for instance in res["instances"]])
    #print("tf_resource_type:" ,tf_resource_type)
    # Create dictionaries for comparison
    if tf_resource_type == "snowflake_user":
        tf_res_map = {res["login_name"]: res for res in tf_resources}
        sf_res_map = {res["login_name"]: res for res in sf_resources}  
    else:
        tf_res_map = {res["name"]: res for res in tf_resources}
        sf_res_map = {res["name"]: res for res in sf_resources}
        


    # Compare resources
    for res_name in set(tf_res_map.keys()) | set(sf_res_map.keys()):
        if res_name not in tf_res_map:
            drifts.append({
                "resource": tf_resource_type,
                "name": res_name,
                "Snowflake": True,
                "Terraform": False
                #"drift": "Exists in Snowflake but not in Terraform"
            })
        elif res_name not in sf_res_map:
            drifts.append({
                "resource": tf_resource_type,
                "name": res_name,
                "Snowflake": False,
                "Terraform": True
                #"drift": "Exists in Terraform but not in Snowflake"
            })
        else:
            # Compare specified attributes
            tf_res = tf_res_map[res_name]
            sf_res = sf_res_map[res_name]
            for attr in attributes:
                tf_value = tf_res.get(attr)
                sf_value = sf_res.get(attr)
                # Normalize case for warehouse_size and wdetearehouse_type
                tf_value_str = str(tf_value).upper() if tf_value is not None else "NULL"
                sf_value_str = str(sf_value).upper() if sf_value is not None else "NULL"
                
                #Check if values are synonyms
                synonyms_for_attr = synonyms.get(resource, {}).get(attr.lower(), {})
                tf_synonym = synonyms_for_attr.get(tf_value_str, tf_value_str)
                sf_synonym = synonyms_for_attr.get(sf_value_str, sf_value_str)

                if tf_synonym != sf_synonym:
                    drifts.append({
                        "resource": tf_resource_type,
                        "name": res_name,
                        "attribute": attr,
                        "Snowflake": sf_value_str,
                        "Terraform": tf_value_str
                        #"drift": f"{attr} mismatch (Terraform: '{tf_value_str}', Snowflake: '{sf_value_str}')"
                    })

    return drifts


def main():
    
    
    # parse input arguments
    inputs = parse_args()

    # Alerts Location
    alerts_locaiton = inputs.alerts_location
    accounts_config_file = inputs.accounts_config_file
    server_cert_path = inputs.server_cert_path
    drift_config = inputs.drift_config
    synonyms_config = inputs.synonyms_config
    tfc_api_base_url = inputs.tfc_api_base_url
    tfc_org =  inputs.tfc_org


    kp = PyKeePass(
            filename=inputs.keepass_db, keyfile=inputs.kp_key_file
        )
    tfc_api_cred = kp.find_entries(title=inputs.tf_token_name, first = True)
    tfc_api_token = tfc_api_cred.password



    headers = {
    "Authorization": f"Bearer {tfc_api_token}",
    "Content-Type": "application/vnd.api+json",
    }

    account_config = os.path.join(alerts_locaiton, accounts_config_file)
    server_cert = os.path.join(alerts_locaiton, server_cert_path)
    

    # Step 1: Get Snowflake Account Config values
    account_config = gc.get_config(account_config)

    
    print("Loading synonyms...")
    synonyms = load_synonyms(synonyms_config)

    for val in account_config:
        vault_url = val["VAULT_URL"]
        secret_path = val["SECRET_PATH"]
        vault_namespace = val["VAULT_NAMESPACE"]
        mount_point = val["MOUNT_POINT"]
        account_name = val["ACCOUNT_NAME"]
        host = val["HOST"]
        user_name = val["USER_NAME"]
        email_recipients = val["EMAIL_RECIPIENTS"]
        sender_email =  val["SENDER_EMAIL"]
        snow_warehouse =  val["SNOWFLAKE_WAREHOUSE"]
        snow_role =  val["SNOWFLAKE_ROLE"]
        snow_db =  val["SNOWFLAKE_DB"]
        tfc_workspace_name  = val["TFC_WORKSPACE_NAME"]


        vault_entries = kp.find_entries(title=user_name, first = True)
        role_id = vault_entries.username
        secret_id = vault_entries.password

        tf_statefile = os.path.join("TerraformStateFile\\", tfc_workspace_name+".tfstate")
        tf_state_local = os.path.join(alerts_locaiton, tf_statefile)

        output_file = os.path.join("Drift_Output\\", tfc_workspace_name+"_drift.json")
        output_file_location = os.path.join(alerts_locaiton, output_file)

        tfc_org = "tfdfagds"
        client = None
        # Step 2: Connect to Vault and authenticate using AppRole
        email_subject = f"Snowflake Account {account_name} Alert: Failed while connecting to HashiCorp Vault"
        client = hconnect.get_vault_client(account_name,vault_url, role_id, secret_id, vault_namespace, email_subject, sender_email, email_recipients, server_cert)
        
        if(client):
            private_key = None
            email_subject = f"Snowflake Account {account_name} Alert: Failed while retrieving Credentials from HashiCorp Vault"
            # Step 3: Retrieve user credentials
            private_key = hconnect.retrieve_user_credentials(account_name, client, secret_path, mount_point, email_subject, sender_email, email_recipients)
            
            if (private_key):
                
                try:
                    # Load resource configuration
                    print("Loading resource configuration...")
                    resource_config = load_resource_config(drift_config)

                    
                    # Read Terraform state
                    tf_state = get_terraform_state(tf_state_local, tfc_org, tfc_workspace_name, tfc_api_base_url, headers)

                    # Process each resource type and Collect drift resulst for all resources
                    output = {}
                    for res_config in resource_config:
                        resource = res_config["Resource"]
                        attributes = res_config["Attributes"]
                        sql_query = res_config["Sql"]
                        
                        print(f"\nQuerying Snowflake {resource}s...")
                        sf_resources = get_snowflake_resources(resource, attributes, sql_query, host, account_name, user_name, private_key, snow_warehouse, snow_role, snow_db)
                        
                        print(f"Comparing {resource}s...")
                        drifts = compare_resources(tf_state, sf_resources, resource, attributes, synonyms)
                        output[resource] = drifts

                    has_drifts = any (drifts for drifts in output.values())

                    if has_drifts:
                        try:
                            with open(output_file_location, "w") as f:
                                json.dump(output, f, indent=2)
                            print(f"\nDrift results written to {output_file_location}")

                            email_subject = f"{account_name}: Snowflake-Terraform Drift Detected"
                            message = f"Snowflake-Terraform Drift File Location {output_file_location}"
                            email.send_email(email_subject, message, sender_email, email_recipients)
                        except Exception as e:
                            print(f"Error writing to output file: {str(e)}")

                    else:
                        print ("\nNo drift detected")  
                    

                except RuntimeError as e:
                    print(f"Error: {str(e)}")
                except Exception as e:
                    print(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()

