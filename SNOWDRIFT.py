from pathlib import Path
import os
import json
import argparse
import smtplib
import logging
from typing import Dict, Any, List, Tuple
from pykeepass import PyKeePass
from utils import config_utils, keepass_utils, mail_utils, snowflake_utils, synonyms_utils, terraform_utils, vault_utils
from components import dependencies, resource_comparison

# Initialize logger
logger = dependencies.setup_logging()
logger = logging.getLogger('app.SNOW_DRIFT')

def parse_args():
    """
    Parse command-line arguments for the application.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Compare Terraform state with Snowflake resources.")
    parser.add_argument("--alerts-location",  metavar="", help="Base directory for alerts and output files")
    parser.add_argument("--accounts-config-file", default="config/accounts.json", metavar="", help="Path to accounts configuration JSON file")
    parser.add_argument("--server-cert-path", default="cert/", metavar="", help="Path to server certificate file")
    parser.add_argument("--drift-config", default="config/drift_resource_attributes.json", metavar="", help="Path to drift configuration JSON file")
    parser.add_argument("--synonyms-config", default="config/synonyms.json", metavar="", help="Path to synonyms configuration JSON file")
    parser.add_argument("--keepass-db", default="",  metavar="", help="Path to KeePass database file")
    parser.add_argument("--kp-key-file", default="", metavar="", help="Path to KeePass key file")
    parser.add_argument("--tfc-api-base-url",  default="https://app.terraform.io/api/v2", metavar="", help="Terraform Cloud API base URL")
    parser.add_argument("--tfc-org", default="", metavar="", help="Terraform Cloud organization name")
    parser.add_argument("--tf-token-name", default="Terraform Team Token",  metavar="", help="Title of Terraform Cloud API token in KeePass")
    parser.add_argument("--send-consolidated-email", action="store_true", help="Send one consolidated email for all accounts instead of individual emails")
    return parser.parse_args()


def main():
    """
    Main function to orchestrate the comparison of Terraform state with Snowflake resources and report drifts.

    Args:
        None: Inputs are parsed from command-line arguments.

    Returns:
        None
    """
    failures : List[str] = []
    account_drift_summary : List[Dict[str, Any]] = []

    try:
        # Parse input arguments
        inputs = parse_args()

        # Set up environment and check dependencies
        dependencies.setup_environment(inputs.alerts_location)

        # Extract and validate input parameters
        alerts_location = Path(inputs.alerts_location).resolve()
        accounts_config_file = inputs.accounts_config_file
        server_cert_path = inputs.server_cert_path
        drift_config = inputs.drift_config
        synonyms_config = inputs.synonyms_config
        tfc_api_base_url = inputs.tfc_api_base_url
        tfc_org = inputs.tfc_org
        send_consolidated_email = inputs.send_consolidated_email

        # Construct configuration file paths relative to alerts_location
        account_config_path = alerts_location / accounts_config_file
        server_cert = alerts_location / server_cert_path
        drift_config_path = alerts_location / drift_config
        synonyms_config_path = alerts_location / synonyms_config


        # Validate input parameters
        for path, name in [
            (account_config_path, "accounts config"),
            (server_cert, "server cert"),
            (drift_config_path, "drift config"),
            (synonyms_config_path, "synonyms config")
        ]:
            if not path.exists():
                error = f"{name} not found. {path}"
                logger.error(error)
                failures.append(error)
                return # Critical: can't proceed

        # Retrieve Terraform Cloud API token from KeePass
        logger.info("Retrieving Terraform Cloud API token from KeePass...")
        tfc_api_cred = keepass_utils.get_keepass_title_cred(inputs.keepass_db, inputs.kp_key_file, inputs.tf_token_name)
        if tfc_api_cred is None or tfc_api_cred.password is None:
            error = "Terraform Cloud API token not found in KeePass"
            logger.error(error)
            failures.append(error)
            return
        tfc_api_token = tfc_api_cred.password
        headers = {
            "Authorization": f"Bearer {tfc_api_token}",
            "Content-Type": "application/vnd.api+json",
        }

        # Load account configuration
        logger.info("Loading account configuration...")
        account_config = config_utils.get_config(str(account_config_path))
        if account_config is None:
            error = "Failed to load snowflake account configuration"
            logger.error(error)
            failures.append(error)
            return

        # Load synonyms
        logger.info("Loading synonyms...")
        synonyms = synonyms_utils.load_synonyms(str(synonyms_config_path))
        if synonyms is None:
            error = "Failed to load snowflake synonyms"
            logger.error(error)
            failures.append(error)
            return
        
        # Load resource configuration
        logger.info("Loading resource configuration...")
        resource_config = config_utils.load_resource_config(str(drift_config_path))
        if resource_config is None:
            error = "Failed to load drift  resource-attributes configuration "
            logger.error(error)
            failures.append(error)
            return

        def process_account(val: Dict[str, Any]) -> Tuple[List[str], Dict[str, Any]]:
            """
            Process a single account and return failures and drift summary.
            
            Returns:
                Tuple containing:
                - List of error messages (account_failures)
                - Dictionary with drift summary for this account
            """
            account_failures = []
            drift_summary = {
                'account_name': '',
                'total_drifts': 0,
                'resource_types': {},
                'output_file': ''
            }
            
            required_fields = ["VAULT_URL", "SECRET_PATH", "VAULT_NAMESPACE", "MOUNT_POINT",
                              "ACCOUNT_NAME", "HOST", "USER_NAME", "EMAIL_RECIPIENTS",
                              "SENDER_EMAIL", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_ROLE",
                              "SNOWFLAKE_DB", "TFC_WORKSPACE_NAME"]
            if not isinstance(val, dict) or not all(field in val for field in required_fields):
                error = "Invalid account configuration: missing required fields"
                logger.error(error)
                return ([error], drift_summary)

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

            drift_summary['account_name'] = account_name

            if not all(isinstance(param, str) and param.strip() for param in [
                vault_url, secret_path, vault_namespace, mount_point, account_name, host,
                user_name, sender_email, snow_warehouse, snow_role, snow_db, tfc_workspace_name
            ]):
                error = f"Invalid configuration for account {account_name}: all fields must be non-empty strings"
                logger.error(error)
                return ([error], drift_summary)
            if not isinstance(email_recipients, (str, list)) or (isinstance(email_recipients, str) and not email_recipients.strip()):
                error = f"Invalid email_recipients for account {account_name}"
                logger.error(error)
                return ([error], drift_summary)

            # Retrieve Vault credentials from KeePass
            logger.info(f"Retrieving Vault credentials for user {user_name}...")
            vault_entries = keepass_utils.get_keepass_title_cred(inputs.keepass_db, inputs.kp_key_file, user_name)
            if vault_entries is None:
                error = f"Vault credentials for user {user_name} not found in KeePass"
                logger.error(error)
                return ([error], drift_summary)
            role_id = vault_entries.username
            secret_id = vault_entries.password

            # Connect to Vault and authenticate
            logger.info(f"Connecting to Vault for account {account_name}...")
            client = vault_utils.get_vault_client(account_name, vault_url, role_id, secret_id, vault_namespace, str(server_cert))
            if client is None:
                error = f"Failed to connect to HashiCorp Vault for account {account_name}"
                logger.error(error)
                return ([error], drift_summary)

            # Retrieve user credentials from Vault
            logger.info(f"Retrieving credentials from Vault for account {account_name}...")
            private_key = vault_utils.retrieve_user_credentials(account_name, client, secret_path, mount_point)
            if private_key is None:
                error = f"Failed to retrieve credentials from Vault for account {account_name}"
                logger.error(error)
                return ([error], drift_summary)

        
            # Download Terraform state 
            tf_statefile = alerts_location / "terraformstatefiles" / f"{tfc_workspace_name.strip()}.tfstate"
            tf_statefile.parent.mkdir(parents=True, exist_ok=True)


            # Retrieve Terraform state manually using individual functions
            logger.info(f"Retrieving workspace ID for {tfc_workspace_name}...")
            workspace_id = terraform_utils.get_workspace_id(tfc_org, tfc_workspace_name, tfc_api_base_url, headers)
            if workspace_id is None:
                error = f"Account {account_name}: Failed to get Terraform workspace ID"
                logger.error(error)
                return ([error], drift_summary)


            logger.info(f"Retrieving state download URL for workspace {workspace_id}...")
            download_url = terraform_utils.get_current_state_download_url(workspace_id, tfc_api_base_url, headers)
            if download_url is None:
                error = f"Account {account_name}: Failed to get Terraform state download URL"
                logger.error(error)
                return ([error], drift_summary)

            logger.info(f"Downloading state file to {tf_statefile}...")
            tf_state = terraform_utils.download_state_file(download_url, str(tf_statefile), headers)
            if tf_state is None:
                error = f"Account {account_name}: Failed to download or parse Terraform state"
                logger.error(error)
                return ([error], drift_summary)

            # Process each resource type and collect drift results
            output = {}

            for res_config in resource_config:
                resource = res_config["Resource"]
                attributes = res_config["Attributes"]
                sql_query = res_config["Sql"]

                logger.info(f"Querying Snowflake {resource}s...")
                sf_resources = snowflake_utils.get_snowflake_resources(
                    resource, attributes, sql_query, host, account_name, user_name,
                    private_key, snow_warehouse, snow_role, snow_db
                )

                if sf_resources is None:
                    error = f"Account {account_name}: Failed to query {resource}'s from Snowflake"
                    logger.error(error)
                    account_failures.append(error)
                    continue


                logger.info(f"Comparing {resource}s...")
                drifts = resource_comparison.compare_resources(tf_state, sf_resources, resource, attributes, synonyms)
                if drifts is None:
                    error = f"Account {account_name}: Failure during drift detection"
                    logger.error(error)
                    account_failures.append(error)
                    continue
                output[resource] = drifts
            
            # Save Drift Output
            output_file = alerts_location / "drift_output" / f"{tfc_workspace_name.strip()}_drift.json"
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Check for drifts and save results
            has_drifts = any(drifts for drifts in output.values())
            if has_drifts:
                try:
                    with open(output_file, "w") as f:
                        json.dump(output, f, indent=2)
                    logger.info(f"Drift results written to {output_file}")
                    
                    # Populate drift summary
                    drift_summary['output_file'] = str(output_file)
                    for resource_type, drifts in output.items():
                        if drifts:
                            drift_summary['resource_types'][resource_type] = len(drifts)
                            drift_summary['total_drifts'] += len(drifts)
                    
                    # Send individual email only if consolidated email is not enabled
                    if not send_consolidated_email:
                        email_subject = f"{account_name}: Snowflake-Terraform Drift Detected"
                        mail_utils.send_drift_email(
                            email_subject, 
                            account_name, 
                            output, 
                            str(output_file), 
                            sender_email, 
                            email_recipients
                        )
                except Exception as file_error:
                    error = f"Error writing to output file {output_file}: {file_error}"
                    logger.error(error)
                    account_failures.append(error)
            
            return (account_failures, drift_summary)

        # Execute account processing in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        with ThreadPoolExecutor() as executor:
            future_to_account = {executor.submit(process_account, val): val for val in account_config}
            for future in as_completed(future_to_account):
                try:
                    account_failures, drift_summary = future.result()
                    failures.extend(account_failures)
                    
                    # Collect drift summaries for consolidated email
                    if drift_summary['total_drifts'] > 0:
                        account_drift_summary.append(drift_summary)
                        
                except Exception as exc:
                    error = f"Account processing generated an exception: {exc}"
                    logger.error(error)
                    failures.append(error)
        
        # Send consolidated email if enabled and there are drifts
        if send_consolidated_email and account_drift_summary:
            logger.info(f"Sending consolidated drift email for {len(account_drift_summary)} account(s)...")
            # Use email settings from first account as default
            if account_config and isinstance(account_config, list) and len(account_config) > 0:
                fallback_sender = account_config[0].get("SENDER_EMAIL")
                fallback_recipients = account_config[0].get("EMAIL_RECIPIENTS")
                if fallback_sender and fallback_recipients:
                    mail_utils.send_consolidated_drift_email(
                        account_drift_summary,
                        fallback_sender,
                        fallback_recipients
                    )
        
        # Final Summary #                    
        logger.info(f"Drift detection completed. Total failures: {len(failures)}")

    except Exception as e:
        error = f"Unexpected error - {e}"
        logger.error(error)
        failures.append(error)

    if failures:
        summary = "\n".join(f". {f}" for f in failures)
        email_subject = f"FAILURE - Snowflake-Terraform Drift Detection"
        message = f""" Drift Detection completed with {len(failures)} failure(s):
        {summary}
        check logs for details
        """
        # Note: Sending failure email to the first account's sender/recipient as a fallback
        # In a real scenario, this might need a dedicated admin email config
        if account_config and isinstance(account_config, list) and len(account_config) > 0:
             fallback_sender = account_config[0].get("SENDER_EMAIL")
             fallback_recipients = account_config[0].get("EMAIL_RECIPIENTS")
             if fallback_sender and fallback_recipients:
                 mail_utils.send_email(email_subject, message, fallback_sender, fallback_recipients)
    else:
        logger.info("No failures detected")

if __name__ == "__main__":
    main()
