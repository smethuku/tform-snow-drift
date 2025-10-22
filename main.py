from pathlib import Path
import os
import json
import argparse
import smtplib
import logging
from typing import Dict, Any, List
from email.mime.text import MIMEText
from pykeepass import PyKeePass
from dependencies import setup_logging, setup_environment
from keepass_utils import get_keepass_title_cred
from config_utils import get_config, load_resource_config
from synonyms_utils import load_synonyms
from workspace_utils import get_workspace_id, get_current_state_download_url
from state_file_utils import download_state_file
from terraform_utils import get_terraform_state
from snowflake_utils import get_snowflake_resources
from resource_comparison import compare_resources
from vault_utils import get_vault_client, retrieve_user_credentials

# Initialize logger
logger = setup_logging()
logger = logging.getLogger('app.main_workflow')

def parse_args():
    """
    Parse command-line arguments for the application.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Compare Terraform state with Snowflake resources.")
    parser.add_argument("--alerts-location", required=True, help="Base directory for alerts and output files")
    parser.add_argument("--accounts-config-file", default="config/accounts.json", help="Path to accounts configuration JSON file relative to alerts-location")
    parser.add_argument("--server-cert-path", default="config/cert.pem", help="Path to server certificate file relative to alerts-location")
    parser.add_argument("--drift-config", default="config/drift_resource_attributes.json", help="Path to drift configuration JSON file relative to alerts-location")
    parser.add_argument("--synonyms-config", default="config/synonyms.json", help="Path to synonyms configuration JSON file relative to alerts-location")
    parser.add_argument("--tfc-api-base-url", required=True, help="Terraform Cloud API base URL")
    parser.add_argument("--tfc-org", required=True, help="Terraform Cloud organization name")
    parser.add_argument("--keepass-db", required=True, help="Path to KeePass database file")
    parser.add_argument("--kp-key-file", required=True, help="Path to KeePass key file")
    parser.add_argument("--tf-token-name", required=True, help="Title of Terraform Cloud API token in KeePass")
    return parser.parse_args()

def send_email(subject: str, message: str, sender_email: str, recipients: str | List[str]) -> None:
    """
    Send an email notification with the specified subject and message.

    Args:
        subject (str): Email subject.
        message (str): Email body.
        sender_email (str): Sender's email address.
        recipients (str | List[str]): Recipient email address(es).

    Raises:
        RuntimeError: If email sending fails.
    """
    try:
        if isinstance(recipients, str):
            recipients = [recipients]
        if not all(isinstance(r, str) and r.strip() for r in recipients):
            raise ValueError("All recipients must be non-empty strings")

        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipients)

        # Use a placeholder SMTP server configuration (update with your SMTP details)
        with smtplib.SMTP('smtp.example.com', 587) as server:
            server.starttls()
            server.login(sender_email, "your_smtp_password")  # Update with actual credentials
            server.send_message(msg)
        logger.info(f"Email sent successfully: {subject}")

    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise RuntimeError(f"Failed to send email: {e}")

def main():
    """
    Main function to orchestrate the comparison of Terraform state with Snowflake resources and report drifts.

    Args:
        None: Inputs are parsed from command-line arguments.

    Returns:
        None

    Raises:
        ValueError: If input arguments or configuration data are invalid.
        RuntimeError: For errors during KeePass access, Vault authentication, Terraform state retrieval,
                      Snowflake queries, or file operations.
    """
    try:
        # Parse input arguments
        inputs = parse_args()

        # Set up environment and check dependencies
        setup_environment(inputs.alerts_location)

        # Extract and validate input parameters
        alerts_location = Path(inputs.alerts_location).resolve()
        accounts_config_file = inputs.accounts_config_file
        server_cert_path = inputs.server_cert_path
        drift_config = inputs.drift_config
        synonyms_config = inputs.synonyms_config
        tfc_api_base_url = inputs.tfc_api_base_url
        tfc_org = inputs.tfc_org

        # Construct configuration file paths relative to alerts_location
        account_config_path = alerts_location / accounts_config_file
        server_cert = alerts_location / server_cert_path
        drift_config_path = alerts_location / drift_config
        synonyms_config_path = alerts_location / synonyms_config

        # Validate input parameters
        string_params = [str(account_config_path), str(server_cert), str(drift_config_path),
                         str(synonyms_config_path), tfc_api_base_url, tfc_org]
        if not all(isinstance(param, str) and param.strip() for param in string_params):
            raise ValueError("All input string parameters must be non-empty strings")
        if not alerts_location.is_dir():
            raise ValueError(f"alerts_location '{alerts_location}' is not a valid directory")
        for config_file in [account_config_path, server_cert, drift_config_path, synonyms_config_path]:
            if not config_file.exists():
                raise ValueError(f"Configuration file '{config_file}' does not exist")

        # Retrieve Terraform Cloud API token from KeePass
        logger.info("Retrieving Terraform Cloud API token from KeePass...")
        tfc_api_cred = get_keepass_title_cred(inputs.keepass_db, inputs.kp_key_file, inputs.tf_token_name)
        if not tfc_api_cred or not tfc_api_cred.password:
            raise ValueError("Terraform Cloud API token not found in KeePass")
        tfc_api_token = tfc_api_cred.password

        headers = {
            "Authorization": f"Bearer {tfc_api_token}",
            "Content-Type": "application/vnd.api+json",
        }

        # Load account configuration
        logger.info("Loading account configuration...")
        account_config = get_config(str(account_config_path))

        # Load synonyms
        logger.info("Loading synonyms...")
        synonyms = load_synonyms(str(synonyms_config_path))

        for val in account_config:
            required_fields = ["VAULT_URL", "SECRET_PATH", "VAULT_NAMESPACE", "MOUNT_POINT",
                              "ACCOUNT_NAME", "HOST", "USER_NAME", "EMAIL_RECIPIENTS",
                              "SENDER_EMAIL", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_ROLE",
                              "SNOWFLAKE_DB", "TFC_WORKSPACE_NAME"]
            if not isinstance(val, dict) or not all(field in val for field in required_fields):
                raise ValueError(f"Invalid account configuration: missing required fields")

            vault_url = val["VAULT_URL"]
            secret_path = val["SECRET_PATH"]
            vault_namespace = val["VAULT_NAMESPACE"]
            mount_point = val["MOUNT_POINT"]
            account_name = val["ACCOUNT_NAME"]
            host = val["HOST"]
            user_name = val["USER_NAME"]
            email_recipients = val["EMAIL_RECIPIENTS"]
            sender_email = val["SENDER_EMAIL"]
            snow_warehouse = val["SNOWFLAKE_WAREHOUSE"]
            snow_role = val["SNOWFLAKE_ROLE"]
            snow_db = val["SNOWFLAKE_DB"]
            tfc_workspace_name = val["TFC_WORKSPACE_NAME"]

            if not all(isinstance(param, str) and param.strip() for param in [
                vault_url, secret_path, vault_namespace, mount_point, account_name, host,
                user_name, sender_email, snow_warehouse, snow_role, snow_db, tfc_workspace_name
            ]):
                raise ValueError(f"Invalid configuration for account {account_name}: all fields must be non-empty strings")
            if not isinstance(email_recipients, (str, list)) or (isinstance(email_recipients, str) and not email_recipients.strip()):
                raise ValueError(f"Invalid email_recipients for account {account_name}")

            # Retrieve Vault credentials from KeePass
            logger.info(f"Retrieving Vault credentials for user {user_name}...")
            vault_entries = get_keepass_title_cred(inputs.keepass_db, inputs.kp_key_file, user_name)
            if not vault_entries or not vault_entries.username or not vault_entries.password:
                raise RuntimeError(f"Vault credentials for user {user_name} not found in KeePass")
            role_id = vault_entries.username
            secret_id = vault_entries.password

            # Construct file paths for Terraform state and drift output
            tf_statefile = alerts_location / "TerraformStateFile" / f"{tfc_workspace_name.strip()}.tfstate"
            output_file = alerts_location / "Drift_Output" / f"{tfc_workspace_name.strip()}_drift.json"

            tf_statefile.parent.mkdir(parents=True, exist_ok=True)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Connect to Vault and authenticate
            email_subject = f"Snowflake Account {account_name} Alert: Failed while connecting to HashiCorp Vault"
            logger.info(f"Connecting to Vault for account {account_name}...")
            client = get_vault_client(account_name, vault_url, role_id, secret_id, vault_namespace, str(server_cert))
            if not client:
                raise RuntimeError(f"Failed to connect to HashiCorp Vault for account {account_name}")

            # Retrieve user credentials from Vault
            email_subject = f"Snowflake Account {account_name} Alert: Failed while retrieving Credentials from HashiCorp Vault"
            logger.info(f"Retrieving credentials from Vault for account {account_name}...")
            private_key = retrieve_user_credentials(account_name, client, secret_path, mount_point)
            if not private_key:
                raise RuntimeError(f"Failed to retrieve credentials from Vault for account {account_name}")

            # Load resource configuration
            logger.info("Loading resource configuration...")
            resource_config = load_resource_config(str(drift_config_path))

            # Retrieve Terraform state manually using individual functions
            logger.info(f"Retrieving workspace ID for {tfc_workspace_name}...")
            workspace_id = get_workspace_id(tfc_org, tfc_workspace_name, tfc_api_base_url, headers)
            logger.info(f"Retrieving state download URL for workspace {workspace_id}...")
            download_url = get_current_state_download_url(workspace_id, tfc_api_base_url, headers)
            logger.info(f"Downloading state file to {tf_statefile}...")
            tf_state = download_state_file(download_url, str(tf_statefile), headers)

            # Process each resource type and collect drift results
            output = {}
            for res_config in resource_config:
                if not isinstance(res_config, dict) or not all(key in res_config for key in ["Resource", "Attributes", "Sql"]):
                    raise ValueError(f"Invalid resource configuration: missing required fields")

                resource = res_config["Resource"]
                attributes = res_config["Attributes"]
                sql_query = res_config["Sql"]

                logger.info(f"Querying Snowflake {resource}s...")
                sf_resources = get_snowflake_resources(
                    resource, attributes, sql_query, host, account_name, user_name,
                    private_key, snow_warehouse, snow_role, snow_db
                )

                logger.info(f"Comparing {resource}s...")
                drifts = compare_resources(tf_state, sf_resources, resource, attributes, synonyms)
                output[resource] = drifts

            # Check for drifts and save results
            has_drifts = any(drifts for drifts in output.values())
            if has_drifts:
                try:
                    with open(output_file, "w") as f:
                        json.dump(output, f, indent=2)
                    logger.info(f"Drift results written to {output_file}")
                    email_subject = f"{account_name}: Snowflake-Terraform Drift Detected"
                    message = f"Snowflake-Terraform Drift File Location: {output_file}"
                    send_email(email_subject, message, sender_email, email_recipients)
                except Exception as file_error:
                    logger.error(f"Error writing to output file {output_file}: {file_error}")
                    raise RuntimeError(f"Error writing to output file {output_file}: {file_error}")
            else:
                logger.info("No drift detected")

    except ValueError as ve:
        logger.error(f"Invalid input or configuration - {ve}")
        raise RuntimeError(f"Invalid input or configuration: {ve}")
    except RuntimeError as re:
        logger.error(f"{re}")
        raise re
    except Exception as e:
        logger.error(f"Unexpected error - {e}")
        raise RuntimeError(f"Unexpected error in main execution: {e}")
