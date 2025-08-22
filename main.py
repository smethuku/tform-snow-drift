import requests
import json
import snowflake.connector
from typing import Dict, Any, List

# Configuration - Replace these with your actual values
# Terraform Cloud details
TFC_ORG = 'your_organization_name'  # e.g., 'my-company'
TFC_WORKSPACE = 'your_workspace_name'  # e.g., 'snowflake-prod'
TFC_TOKEN = 'your_terraform_cloud_token'  # API token from Terraform Cloud

# Snowflake details
SNOWFLAKE_ACCOUNT = 'your_snowflake_account'  # e.g., 'abc12345.us-east-1'
SNOWFLAKE_USER = 'your_snowflake_user'
SNOWFLAKE_PASSWORD = 'your_snowflake_password'
SNOWFLAKE_WAREHOUSE = 'your_warehouse'  # Optional if not needed for queries
SNOWFLAKE_ROLE = 'your_role'  # Optional, e.g., 'ACCOUNTADMIN'

# Function to fetch Terraform state as JSON
def get_terraform_state() -> Dict[str, Any]:
    """
    Fetches the current Terraform state from Terraform Cloud API.
    Assumes the workspace manages Snowflake resources via the Snowflake provider.
    """
    # Step 1: Get the current state version
    state_version_url = f"https://app.terraform.io/api/v2/organizations/{TFC_ORG}/workspaces/{TFC_WORKSPACE}/current-state-version"
    headers = {
        "Authorization": f"Bearer {TFC_TOKEN}",
        "Content-Type": "application/vnd.api+json"
    }
    response = requests.get(state_version_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to get state version: {response.text}")
    
    # Step 2: Extract the hosted state download URL
    state_download_url = response.json()['data']['attributes']['hosted-state-download-url']
    
    # Step 3: Download the state file (JSON)
    state_response = requests.get(state_download_url, headers=headers)
    if state_response.status_code != 200:
        raise Exception(f"Failed to download state: {state_response.text}")
    
    return state_response.json()

# Function to fetch Snowflake resources as JSON
def get_snowflake_resources() -> Dict[str, Any]:
    """
    Connects to Snowflake and retrieves metadata about resources (e.g., databases).
    You can extend this to include more SHOW commands for other resources like tables, users, etc.
    Returns a dictionary representing the resources in JSON-compatible format.
    """
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )
    cur = conn.cursor()
    
    resources = {}
    
    # Example: Fetch databases
    cur.execute("SHOW DATABASES")
    databases = [dict(row) for row in cur.fetchall()]  # Convert to list of dicts
    resources['databases'] = {db['name']: db for db in databases}
    
    # Example: Fetch warehouses (extend as needed)
    cur.execute("SHOW WAREHOUSES")
    warehouses = [dict(row) for row in cur.fetchall()]
    resources['warehouses'] = {wh['name']: wh for wh in warehouses}
    
    # Add more resource types here, e.g., SHOW TABLES IN DATABASE <db>, SHOW USERS, etc.
    # For tables, you'd need to loop over databases and query per database.
    
    cur.close()
    conn.close()
    
    return resources

# Function to compare Terraform state and Snowflake resources for drifts
def detect_drifts(tf_state: Dict[str, Any], sf_resources: Dict[str, Any]) -> List[str]:
    """
    Compares the Terraform state (intended) with actual Snowflake resources.
    Focuses on Snowflake resources managed by Terraform (e.g., snowflake_database, snowflake_warehouse).
    Identifies drifts like missing resources, attribute mismatches, or extras.
    This is a basic implementation; extend based on your specific resources.
    """
    drifts = []
    
    # Extract Snowflake-related resources from Terraform state
    tf_snowflake_resources = {}
    for resource in tf_state.get('resources', []):
        if 'snowflake' in resource['provider']:  # Filter for Snowflake provider
            res_type = resource['type']
            res_name = resource['name']
            for instance in resource['instances']:
                attributes = instance['attributes']
                key = f"{res_type}.{res_name}"
                tf_snowflake_resources[key] = attributes
    
    # Compare databases (example)
    for key, attrs in tf_snowflake_resources.items():
        if key.startswith('snowflake_database.'):
            db_name = attrs.get('name')
            if db_name not in sf_resources.get('databases', {}):
                drifts.append(f"Database '{db_name}' exists in Terraform but not in Snowflake.")
            else:
                sf_db = sf_resources['databases'][db_name]
                # Compare attributes (e.g., comment, retention time)
                if attrs.get('comment') != sf_db.get('comment'):
                    drifts.append(f"Comment mismatch for database '{db_name}': TF='{attrs.get('comment')}', SF='{sf_db.get('comment')}'")
                # Add more attribute comparisons as needed
    
    # Compare warehouses (example)
    for key, attrs in tf_snowflake_resources.items():
        if key.startswith('snowflake_warehouse.'):
            wh_name = attrs.get('name')
            if wh_name not in sf_resources.get('warehouses', {}):
                drifts.append(f"Warehouse '{wh_name}' exists in Terraform but not in Snowflake.")
            else:
                sf_wh = sf_resources['warehouses'][wh_name]
                if attrs.get('warehouse_size') != sf_wh.get('size'):
                    drifts.append(f"Size mismatch for warehouse '{wh_name}': TF='{attrs.get('warehouse_size')}', SF='{sf_wh.get('size')}'")
                # Add more
    
    # Check for extras in Snowflake not in Terraform (optional, for unmanaged resources)
    for db_name in sf_resources.get('databases', {}):
        if not any(key.startswith('snowflake_database.') and attrs.get('name') == db_name for key, attrs in tf_snowflake_resources.items()):
            drifts.append(f"Database '{db_name}' exists in Snowflake but not managed by Terraform.")
    
    # Extend for other resource types...
    
    return drifts

# Main execution
if __name__ == "__main__":
    try:
        # Step 1: Get Terraform state
        tf_state = get_terraform_state()
        print("Terraform state retrieved successfully.")
        
        # Optional: Save to file
        # with open('terraform_state.json', 'w') as f:
        #     json.dump(tf_state, f, indent=4)
        
        # Step 2: Get Snowflake resources
        sf_resources = get_snowflake_resources()
        print("Snowflake resources retrieved successfully.")
        
        # Optional: Save to file
        # with open('snowflake_resources.json', 'w') as f:
        #     json.dump(sf_resources, f, indent=4)
        
        # Step 3: Detect drifts
        drifts = detect_drifts(tf_state, sf_resources)
        
        if drifts:
            print("Drifts detected:")
            for drift in drifts:
                print(f"- {drift}")
        else:
            print("No drifts detected. Infrastructure is in sync.")
    except Exception as e:
        print(f"Error: {str(e)}")