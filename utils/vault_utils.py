import hvac
import logging
from components import dependencies

# Initialize logger
logger = dependencies.setup_logging()
logger = logging.getLogger('app.vault_utils')


def get_vault_client(account_name: str, vault_url: str, role_id: str, secret_id: str, vault_namespace: str, server_cert: str) -> hvac.Client:
    """
    Authenticate with HashiCorp Vault using AppRole and return an authenticated client.

    Args:
        account_name (str): Snowflake account identifier, used for error messaging.
        vault_url (str): URL of the HashiCorp Vault instance.
        role_id (str): Role ID for AppRole authentication.
        secret_id (str): Secret ID for AppRole authentication.
        vault_namespace (str): Namespace within the Vault instance.
        server_cert (str): Path to the server certificate file for verifying the Vault connection.

    Returns:
        hvac.Client: An authenticated Vault client instance, or None if authentication fails.
    """
    try:
        # Validate input parameters
        string_params = [account_name, vault_url, role_id, secret_id, vault_namespace, server_cert]
        if not all(isinstance(param, str) and param.strip() for param in string_params):
            logger.error("All input parameters must be non-empty strings")
            return None

        # Initialize Vault client
        client = hvac.Client(url=vault_url, namespace=vault_namespace, verify=server_cert)

        # Authenticate using AppRole
        app_role_auth = client.auth.approle.login(role_id=role_id, secret_id=secret_id)
        if not app_role_auth.get('auth'):
            logger.error("Vault authentication response missing 'auth' field")
            return None

        logger.info(f"Successfully authenticated to Vault for account {account_name}")
        return client

    except ValueError as ve:
        logger.error(f"Snowflake Account {account_name}: Invalid input - {ve}")
        return None
    except hvac.exceptions.VaultError as vault_error:
        logger.error(f"Snowflake Account {account_name}: Failed to authenticate to HashiCorp Vault - {vault_error}")
        return None
    except Exception as e:
        logger.error(f"Snowflake Account {account_name}: Unexpected error during Vault authentication - {e}")
        return None



def retrieve_user_credentials(account_name: str, client: hvac.Client, secret_path: str, mount_point: str) -> str:
    """
    Retrieve user credentials (RSA private key) from HashiCorp Vault.

    Args:
        account_name (str): Snowflake account identifier, used for error messaging.
        client (hvac.Client): Authenticated Vault client instance.
        secret_path (str): Path to the secret in Vault.
        mount_point (str): Mount point for the Vault secrets engine.

    Returns:
        str: The RSA private key retrieved from Vault, or None if retrieval fails.
    """
    try:
        # Validate input parameters
        if not isinstance(client, hvac.Client):
            logger.error("client must be a valid hvac.Client instance")
            return None
        if not all(isinstance(param, str) and param.strip() for param in [secret_path, mount_point]):
            logger.error("secret_path and mount_point must be non-empty strings")
            return None

        # Read the secret from Vault
        read_response = client.secrets.kv.v1.read_secret(path=secret_path, mount_point=mount_point)

        # Verify the response contains the expected key
        if not isinstance(read_response, dict) or 'data' not in read_response:
            logger.error("Invalid Vault response: 'data' field missing")
            return None
        if 'rsa_private_key' not in read_response['data']:
            logger.error("Secret response missing 'rsa_private_key' field")
            return None

        private_key = read_response['data']['rsa_private_key']
        if not isinstance(private_key, str) or not private_key.strip():
            logger.error("Retrieved rsa_private_key is empty or invalid")
            return None

        return private_key

    except ValueError as ve:
        logger.error(f"Snowflake Account {account_name}: Invalid input - {ve}")
        return None
    except hvac.exceptions.VaultError as vault_error:
        logger.error(f"Snowflake Account {account_name}: Failed to retrieve user credentials from HashiCorp Vault - {vault_error}")
        return None
    except RuntimeError as re:
        logger.error(f"Snowflake Account {account_name}: {re}")
        return None
    except Exception as e:
        logger.error(f"Snowflake Account {account_name}: Unexpected error retrieving credentials - {e}")
        return None