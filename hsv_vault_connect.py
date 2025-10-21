import hvac
def get_vault_client(account_name: str, vault_url: str, role_id: str, secret_id: str, vault_namespace:str, server_cert: str):
    """
    Authenticate with Vault using AppRole and return the client.

    Args:
        account_name (str): Snowflake Account Identified.
        vault_url (str): Hashicorp Vault URL.
        role_id (str): RoleId used to authenticate to the Vault.
        secret_id (str): SecretId used to authenticate to the vault.
        vault_namespace (str): Namespace inside the vault.
        server_cert (str): Server Certificate used to connect to the vault.
    """
    
    try:
        client = hvac.Client(url=vault_url, namespace=vault_namespace, verify=server_cert)
        app_role_auth = client.auth.approle.login(role_id=role_id, secret_id=secret_id)
        if app_role_auth['auth']:
            print("Successfully authenticated to Vault")
    except Exception as e:
        print("Error [get_vault_client]: Snowflake Account {account_name}: Failed to authenticate HashiCorp Vault with the provided App Role and Secret {e}")
    return client


def retrieve_user_credentials(account_name, client, secret_path, mount_pt):
    #Retrieve user credentials from Vault.
    try: 
        read_response = client.secrets.kv.v1.read_secret(path=secret_path, mount_point=mount_pt)        
    except Exception as e:
        print("Error [retrieve_user_credentials]: Snowflake Account {account_name}: Failed to retrieve user credentials from HashiCorp Vault {e}")
    return read_response['data']['rsa_private_key']
