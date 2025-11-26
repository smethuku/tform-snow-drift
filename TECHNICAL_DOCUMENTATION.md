# Terraform-Snowflake Drift Detection Tool

## 1. Project Overview

The **Terraform-Snowflake Drift Detection Tool** is a Python-based utility designed to identify discrepancies ("drifts") between the desired state of Snowflake resources defined in Terraform and their actual state in the Snowflake environment.

This tool helps DevOps and Data Engineering teams ensure infrastructure-as-code (IaC) integrity by:
1.  Fetching the latest Terraform state from Terraform Cloud (TFC).
2.  Querying the actual resource configurations from Snowflake.
3.  Comparing the two datasets based on configurable attributes.
4.  Alerting stakeholders via email if any drifts are detected.

## 2. Architecture

The application follows a linear workflow orchestrated by the main script `SNOWDRIFT.py`.

### High-Level Workflow
1.  **Initialization**: The script parses command-line arguments and sets up logging.
2.  **Configuration Loading**: It reads account details, drift definitions, and synonym mappings from JSON config files.
3.  **Authentication**:
    *   **KeePass**: Retrieves the Terraform Cloud API token and Vault credentials (role ID, secret ID).
    *   **HashiCorp Vault**: Authenticates using the retrieved credentials and fetches the Snowflake private key.
4.  **State Retrieval**: Connects to Terraform Cloud API to download the latest `.tfstate` file for the specified workspace.
5.  **Resource Querying**: Connects to Snowflake using the private key and executes SQL queries to fetch current resource attributes (e.g., Warehouses, Databases, Users).
6.  **Comparison**: The `resource_comparison` component iterates through resources and attributes, comparing Terraform values against Snowflake values. It handles value normalization (e.g., case sensitivity) using a synonyms dictionary.
7.  **Reporting**:
    *   If drift is found, a JSON report is generated in the `drift_output/` directory.
    *   An email notification is sent to configured recipients with the drift details.

### Directory Structure
*   `SNOWDRIFT.py`: The entry point and main orchestrator.
*   `components/`: Contains core business logic.
    *   `resource_comparison.py`: Logic for comparing Terraform state vs. Snowflake resources.
    *   `dependencies.py`: Environment setup and logging configuration.
*   `utils/`: Helper modules for external integrations.
    *   `terraform_utils.py`: Interactions with Terraform Cloud API.
    *   `snowflake_utils.py`: Snowflake connection and querying.
    *   `vault_utils.py`: HashiCorp Vault authentication and secret retrieval.
    *   `keepass_utils.py`: KeePass database interaction.
    *   `mail_utils.py`: SMTP email sending.
    *   `config_utils.py`: Configuration file parsers.
*   `config/`: Configuration files (see Setup section).
*   `cert/`: Stores server certificates for Vault connection.

## 3. Prerequisites & Dependencies

### System Requirements
*   **Python 3.8+**
*   **HashiCorp Vault**: For secure storage of Snowflake private keys.
*   **KeePass**: For storing Vault credentials and Terraform Cloud tokens.
*   **Terraform Cloud**: Hosting the state files.
*   **Snowflake**: The target data warehouse.

### Python Libraries
Install dependencies using the `requirements.txt` file:
```bash
pip install -r requirements.txt
```
Key libraries include:
*   `snowflake-connector-python`: For Snowflake connectivity.
*   `hvac`: For HashiCorp Vault interaction.
*   `pykeepass`: For reading KeePass databases.
*   `cryptography`: For handling private keys.

## 4. Setup and Configuration

### 4.1. Configuration Files (`config/`)

#### `accounts.json`
Defines the connection details for Vault, Snowflake, and Terraform Cloud.
```json
[
  {
    "VAULT_URL": "https://vault.example.com",
    "SECRET_PATH": "secret/data/snowflake/user_key",
    "VAULT_NAMESPACE": "admin",
    "MOUNT_POINT": "secret",
    "ACCOUNT_NAME": "snowflake_account_id",
    "HOST": "account.snowflakecomputing.com",
    "USER_NAME": "snowflake_user",
    "EMAIL_RECIPIENTS": ["admin@example.com"],
    "SENDER_EMAIL": "drift-bot@example.com",
    "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
    "SNOWFLAKE_ROLE": "SYSADMIN",
    "SNOWFLAKE_DB": "DEMO_DB",
    "TFC_WORKSPACE_NAME": "my-terraform-workspace"
  }
]
```

#### `drift_resource_attributes.json`
Specifies which resources to monitor and the SQL queries to fetch them.
```json
[
  {
    "Resource": "Warehouse",
    "Attributes": ["name", "warehouse_size", "auto_suspend"],
    "Sql": "SHOW WAREHOUSES; SELECT \"name\", \"size\" as warehouse_size, \"auto_suspend\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));"
  }
]
```

#### `synonyms.json`
Maps values that are semantically equivalent but syntactically different (e.g., boolean values).
```json
{
  "Warehouse": {
    "auto_resume": {
      "TRUE": "true",
      "FALSE": "false"
    }
  }
}
```

### 4.2. Credential Setup

1.  **KeePass**:
    *   Create an entry with the title matching `tf-token-name` (default: "Terraform Team Token") containing the TFC API Token.
    *   Create an entry with the title matching the `USER_NAME` from `accounts.json`. The username field should be the **Vault Role ID** and the password field should be the **Vault Secret ID**.

2.  **Vault**:
    *   Store the Snowflake user's private key (PEM format) at the `SECRET_PATH` specified in `accounts.json`.

## 5. Usage Guide

Run the tool from the command line.

### Basic Usage
```bash
python SNOWDRIFT.py \
  --keepass-db /path/to/database.kdbx \
  --kp-key-file /path/to/keyfile.key \
  --alerts-location /path/to/output_dir
```

### Command-Line Arguments

| Argument | Default | Description |
| :--- | :--- | :--- |
| `--alerts-location` | (Required) | Base directory for logs, alerts, and output files. |
| `--keepass-db` | `""` | Path to the KeePass database file (.kdbx). |
| `--kp-key-file` | `""` | Path to the KeePass key file. |
| `--accounts-config-file` | `config/accounts.json` | Path to the accounts configuration file. |
| `--drift-config` | `config/drift_resource_attributes.json` | Path to the resource attributes configuration. |
| `--synonyms-config` | `config/synonyms.json` | Path to the synonyms configuration. |
| `--server-cert-path` | `cert/` | Path to the directory containing the Vault server certificate. |
| `--tfc-api-base-url` | `https://app.terraform.io/api/v2` | Terraform Cloud API base URL. |
| `--tfc-org` | `""` | Terraform Cloud Organization name. |
| `--tf-token-name` | `Terraform Team Token` | Title of the TFC token entry in KeePass. |

### Output
*   **Console Logs**: Execution progress and errors.
*   **Drift Report**: JSON files saved to `<alerts-location>/drift_output/<workspace>_drift.json`.
*   **Email**: Sent to `EMAIL_RECIPIENTS` if drift is detected or if a critical failure occurs.

## 6. Troubleshooting

*   **"Vault credentials not found"**: Ensure the KeePass entry title matches the `USER_NAME` in `accounts.json`.
*   **"Failed to query Snowflake"**: Check the SQL query in `drift_resource_attributes.json` and ensure the Snowflake user has the correct role and permissions.
*   **"Terraform Cloud API token not found"**: Verify the `tf-token-name` argument matches the KeePass entry title.
