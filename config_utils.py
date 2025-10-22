import json
import logging
from typing import Dict, Any
from dependencies import setup_logging

# Initialize logger
logger = setup_logging()
logger = logging.getLogger('app.config_utils')

def get_config(path: str) -> Dict[str, Any]:
    """
    Load and parse configuration data from a JSON file.

    Args:
        path (str): Path to the JSON configuration file.

    Returns:
        Dict[str, Any]: A dictionary containing the parsed configuration data.

    Raises:
        ValueError: If the path is empty or not a string.
        FileNotFoundError: If the specified configuration file does not exist.
        json.JSONDecodeError: If the file contains invalid JSON.
        RuntimeError: For other unexpected errors during file reading or parsing.
    """
    try:
        # Validate input parameter
        if not isinstance(path, str) or not path.strip():
            raise ValueError("Path must be a non-empty string")

        # Open and read the JSON configuration file
        with open(path, "r") as config_file:
            config = json.load(config_file)

        # Verify that the loaded data is a dictionary
        if not isinstance(config, dict):
            raise ValueError("Configuration file must contain a dictionary")

        return config

    except FileNotFoundError as fnf_error:
        logger.error(f"Configuration file not found - {fnf_error}")
        raise RuntimeError(f"Configuration file not found: {fnf_error}")
    except json.JSONDecodeError as json_error:
        logger.error(f"Invalid JSON format in config file - {json_error}")
        raise RuntimeError(f"Invalid JSON format in config file: {json_error}")
    except ValueError as ve:
        logger.error(f"Invalid input or data format - {ve}")
        raise RuntimeError(f"Invalid input or data format: {ve}")
    except Exception as e:
        logger.error(f"Failed to load configuration - {e}")
        raise RuntimeError(f"Failed to load configuration: {e}")

def load_resource_config(resource_config: str) -> List[Dict[str, Any]]:
    """
    Load and parse resource attributes from a JSON configuration file.

    Args:
        resource_config (str): Path to the JSON file containing resource attributes.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the parsed resource attributes.

    Raises:
        ValueError: If the resource_config path is empty or not a string.
        FileNotFoundError: If the specified configuration file does not exist.
        json.JSONDecodeError: If the file contains invalid JSON.
        RuntimeError: For other unexpected errors during file reading or parsing.
    """
    try:
        # Validate input parameter
        if not isinstance(resource_config, str) or not resource_config.strip():
            raise ValueError("resource_config must be a non-empty string")

        # Open and read the JSON configuration file
        with open(resource_config, "r") as f:
            config_data = json.load(f)
            
        # Verify that the loaded data is a list of dictionaries
        if not isinstance(config_data, list) or not all(isinstance(item, dict) for item in config_data):
            raise ValueError("Configuration file must contain a list of dictionaries")
            
        return config_data

    except FileNotFoundError as fnf_error:
        logger.error(f"Configuration file not found - {fnf_error}")
        raise RuntimeError(f"Configuration file not found: {fnf_error}")
    except json.JSONDecodeError as json_error:
        logger.error(f"Invalid JSON format in config file - {json_error}")
        raise RuntimeError(f"Invalid JSON format in config file: {json_error}")
    except ValueError as ve:
        logger.error(f"Invalid input or data format - {ve}")
        raise RuntimeError(f"Invalid input or data format: {ve}")
    except Exception as e:
        logger.error(f"Failed to load configuration - {e}")
        raise RuntimeError(f"Failed to load configuration: {e}")
