import requests
import json
import logging
from typing import Dict, Any
from dependencies import setup_logging

# Initialize logger
logger = setup_logging()
logger = logging.getLogger('app.workspace_utils')




def get_workspace_id(org_name: str, workspace_name: str, tfc_api_base_url: str, headers: Dict[str, Any]) -> str:
    """
    Retrieve the workspace ID for a given workspace name within an organization using the Terraform Cloud API.

    Args:
        org_name (str): The name of the organization in Terraform Cloud.
        workspace_name (str): The name of the workspace to find.
        tfc_api_base_url (str): The base URL for the Terraform Cloud API.
        headers (Dict[str, Any]): HTTP headers for the API request, typically including authentication.

    Returns:
        str: The ID of the workspace matching the provided name.

    Raises:
        ValueError: If org_name, workspace_name, or tfc_api_base_url is empty or not a string, or if the workspace is not found.
        requests.exceptions.HTTPError: If the API request fails with a non-2xx status code.
        requests.exceptions.RequestException: For network-related errors during the API request.
        RuntimeError: For other unexpected errors during the operation.
    """
    try:
        # Validate input parameters
        if not all(isinstance(param, str) and param.strip() for param in [org_name, workspace_name, tfc_api_base_url]):
            raise ValueError("org_name, workspace_name, and tfc_api_base_url must be non-empty strings")
        if not isinstance(headers, dict):
            raise ValueError("headers must be a dictionary")

        # Construct the API URL for the organization's workspaces
        url = f"{tfc_api_base_url.rstrip('/')}/organizations/{org_name}/workspaces"

        # Make the API request to retrieve workspaces
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse the response JSON and extract workspace data
        workspaces = response.json().get("data", [])
        if not isinstance(workspaces, list):
            raise ValueError("API response 'data' field must be a list")

        # Search for the workspace by name
        for ws in workspaces:
            if not isinstance(ws, dict) or "attributes" not in ws or "id" not in ws:
                continue
            if ws["attributes"].get("name") == workspace_name:
                return ws["id"]

        raise ValueError(f"Workspace '{workspace_name}' not found in organization '{org_name}'")

    except requests.exceptions.HTTPError as http_error:
        logger.error(f"API request failed - {http_error}")
        raise RuntimeError(f"API request failed: {http_error}")
    except requests.exceptions.RequestException as req_error:
        logger.error(f"Network error during API request - {req_error}")
        raise RuntimeError(f"Network error during API request: {req_error}")
    except ValueError as ve:
        logger.error(f"Invalid input or data format - {ve}")
        raise RuntimeError(f"Invalid input or data format: {ve}")
    except Exception as e:
        logger.error(f"Failed to retrieve workspace ID - {e}")
        raise RuntimeError(f"Failed to retrieve workspace ID: {e}")



def get_current_state_download_url(workspace_id: str, tfc_api_base_url: str, headers: Dict[str, Any]) -> str:
    """
    Retrieve the download URL for the current state file of a workspace from Terraform Cloud API.

    Args:
        workspace_id (str): The ID of the workspace in Terraform Cloud.
        tfc_api_base_url (str): The base URL for the Terraform Cloud API.
        headers (Dict[str, Any]): HTTP headers for the API request, typically including authentication.

    Returns:
        str: The URL for downloading the current state file.

    Raises:
        ValueError: If workspace_id or tfc_api_base_url is empty or not a string, or if headers is not a dictionary.
        requests.exceptions.HTTPError: If the API request fails with a non-2xx status code.
        requests.exceptions.RequestException: For network-related errors during the API request.
        KeyError: If the expected JSON structure is missing required fields.
        RuntimeError: For other unexpected errors during the operation.
    """
    try:
        # Validate input parameters
        if not all(isinstance(param, str) and param.strip() for param in [workspace_id, tfc_api_base_url]):
            raise ValueError("workspace_id and tfc_api_base_url must be non-empty strings")
        if not isinstance(headers, dict):
            raise ValueError("headers must be a dictionary")

        # Construct the API URL for the current state version
        url = f"{tfc_api_base_url.rstrip('/')}/workspaces/{workspace_id}/current-state-version"

        # Make the API request to retrieve the current state version
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse the response JSON and extract the download URL
        response_data = response.json()
        if not isinstance(response_data, dict) or "data" not in response_data:
            raise KeyError("Invalid API response: 'data' field missing")
        if not isinstance(response_data["data"], dict) or "attributes" not in response_data["data"]:
            raise KeyError("Invalid API response: 'attributes' field missing")
        if "hosted-state-download-url" not in response_data["data"]["attributes"]:
            raise KeyError("Invalid API response: 'hosted-state-download-url' field missing")

        return response_data["data"]["attributes"]["hosted-state-download-url"]

    except requests.exceptions.HTTPError as http_error:
        logger.error(f"API request failed - {http_error}")
        raise RuntimeError(f"API request failed: {http_error}")
    except requests.exceptions.RequestException as req_error:
        logger.error(f"Network error during API request - {req_error}")
        raise RuntimeError(f"Network error during API request: {req_error}")
    except ValueError as ve:
        logger.error(f"Invalid input - {ve}")
        raise RuntimeError(f"Invalid input: {ve}")
    except KeyError as ke:
        logger.error(f"Missing required field in response - {ke}")
        raise RuntimeError(f"Missing required field in response: {ke}")
    except Exception as e:
        logger.error(f"Failed to retrieve state download URL - {e}")
        raise RuntimeError(f"Failed to retrieve state download URL: {e}")
    

def download_state_file(download_url: str, tf_statefilename: str, headers: Dict[str, Any]) -> Dict[str, Any]:
    """
    Download a Terraform state file from a given URL and save it locally, then load and return its contents.

    Args:
        download_url (str): The URL to download the Terraform state file from.
        tf_statefilename (str): The local file path where the state file will be saved.
        headers (Dict[str, Any]): HTTP headers for the API request, typically including authentication.

    Returns:
        Dict[str, Any]: The parsed JSON content of the downloaded state file.

    Raises:
        ValueError: If download_url or tf_statefilename is empty or not a string, or if headers is not a dictionary.
        requests.exceptions.HTTPError: If the API request fails with a non-2xx status code.
        requests.exceptions.RequestException: For network-related errors during the API request.
        FileNotFoundError: If the local file cannot be written or read.
        json.JSONDecodeError: If the downloaded file is not valid JSON.
        RuntimeError: For other unexpected errors during the operation.
    """
    try:
        # Validate input parameters
        if not all(isinstance(param, str) and param.strip() for param in [download_url, tf_statefilename]):
            raise ValueError("download_url and tf_statefilename must be non-empty strings")
        if not isinstance(headers, dict):
            raise ValueError("headers must be a dictionary")

        # Download the state file from the provided URL
        response = requests.get(download_url, headers=headers)
        response.raise_for_status()

        # Save the response content to the specified file
        with open(tf_statefilename, "wb") as f:
            f.write(response.content)

        # Read and parse the saved file as JSON
        with open(tf_statefilename, "r") as f:
            state_data = json.load(f)

        # Verify that the loaded data is a dictionary
        if not isinstance(state_data, dict):
            raise ValueError("State file must contain a JSON dictionary")

        return state_data

    except requests.exceptions.HTTPError as http_error:
        logger.error(f"API request failed - {http_error}")
        raise RuntimeError(f"API request failed: {http_error}")
    except requests.exceptions.RequestException as req_error:
        logger.error(f"Network error during API request - {req_error}")
        raise RuntimeError(f"Network error during API request: {req_error}")
    except FileNotFoundError as fnf_error:
        logger.error(f"File operation failed - {fnf_error}")
        raise RuntimeError(f"File operation failed: {fnf_error}")
    except json.JSONDecodeError as json_error:
        logger.error(f"Invalid JSON format in state file - {json_error}")
        raise RuntimeError(f"Invalid JSON format in state file: {json_error}")
    except ValueError as ve:
        logger.error(f"Invalid input or data format - {ve}")
        raise RuntimeError(f"Invalid input or data format: {ve}")
    except Exception as e:
        logger.error(f"Failed to download or parse state file - {e}")
        raise RuntimeError(f"Failed to download or parse state file: {e}")



def get_terraform_state(tf_state_local: str, tfc_org: str, tfc_workspace_name: str, tfc_api_base_url: str, headers: Dict[str, Any]) -> Dict[str, Any]:
    """
    Retrieve and download the Terraform state file for a specified workspace from Terraform Cloud.

    Args:
        tf_state_local (str): Local file path where the Terraform state file will be saved.
        tfc_org (str): The name of the organization in Terraform Cloud.
        tfc_workspace_name (str): The name of the workspace in Terraform Cloud.
        tfc_api_base_url (str): The base URL for the Terraform Cloud API.
        headers (Dict[str, Any]): HTTP headers for API requests, typically including authentication.

    Returns:
        Dict[str, Any]: The parsed JSON content of the downloaded Terraform state file.

    Raises:
        ValueError: If any string input is empty or not a string, or if headers is not a dictionary.
        RuntimeError: For errors during workspace ID retrieval, state download URL retrieval, or state file download/parsing.
    """
    try:
        # Validate input parameters
        if not all(isinstance(param, str) and param.strip() for param in [tf_state_local, tfc_org, tfc_workspace_name, tfc_api_base_url]):
            raise ValueError("tf_state_local, tfc_org, tfc_workspace_name, and tfc_api_base_url must be non-empty strings")
        if not isinstance(headers, dict):
            raise ValueError("headers must be a dictionary")

        # Step 1: Retrieve the workspace ID
        workspace_id = get_workspace_id(tfc_org, tfc_workspace_name, tfc_api_base_url, headers)

        # Step 2: Get the download URL for the current state file
        download_url = get_current_state_download_url(workspace_id, tfc_api_base_url, headers)

        # Step 3: Download the state file and return its contents
        return download_state_file(download_url, tf_state_local, headers)

    except ValueError as ve:
        logger.error(f"Configuration error - {ve}")
        raise RuntimeError(f"Configuration error: {ve}")
    except RuntimeError as re:
        logger.error(f"{re}")
        raise re
    except requests.exceptions.RequestException as req_error:
        logger.error(f"Network error during API request - {req_error}")
        raise RuntimeError(f"Network error during API request: {req_error}")
    except Exception as e:
        logger.error(f"Failed to retrieve or process Terraform state - {e}")
        raise RuntimeError(f"Failed to retrieve or process Terraform state: {e}")
