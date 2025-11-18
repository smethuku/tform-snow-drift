import json
import logging
from typing import Dict, List, Any
from components import dependencies

# Initialize logger
logger = dependencies.setup_logging()
logger = logging.getLogger('app.config_utils')

def get_config(path: str) -> Dict[str, Any]:
    """
    Load and parse configuration data from a JSON file.

    Args:
        path (str): Path to the JSON configuration file.

    Returns:
        Dict[str, Any]: A dictionary containing the parsed configuration data.
    """
    try:
        # Validate input parameter
        if not isinstance(path, str) or not path.strip():
            logger.error("Path must be a non-empty string")
            return None

        # Open and read the JSON configuration file
        with open(path, "r") as config_file:
            config = json.load(config_file)

        return config

    except FileNotFoundError as fnf_error:
        logger.error(f"Configuration file not found - {fnf_error}")
        return None
    except json.JSONDecodeError as json_error:
        logger.error(f"Invalid JSON format in config file - {json_error}")
        return None
    except ValueError as ve:
        logger.error(f"Invalid input or data format - {ve}")
        return None
    except Exception as e:
        logger.error(f"Failed to load configuration - {e}")
        return None
    
def load_resource_config(resource_config: str) -> List[Dict[str, Any]]:
    """
    Load and parse resource attributes from a JSON configuration file.

    Args:
        resource_config (str): Path to the JSON file containing resource attributes.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the parsed resource attributes.
    """
    try:
        # Validate input parameter
        if not isinstance(resource_config, str) or not resource_config.strip():
            logger.error("resource_config must be a non-empty string")
            return None

        # Open and read the JSON configuration file
        with open(resource_config, "r") as f:
            config_data = json.load(f)
            
        # Verify that the loaded data is a list of dictionaries
        if not isinstance(config_data, list) or not all(isinstance(item, dict) for item in config_data):
            logger.error("Configuration file must contain a list of dictionaries")
            return None
            
        return config_data

    except FileNotFoundError as fnf_error:
        logger.error(f"Configuration file not found - {fnf_error}")
        return None
    except json.JSONDecodeError as json_error:
        logger.error(f"Invalid JSON format in config file - {json_error}")
        return None
    except ValueError as ve:
        logger.error(f"Invalid input or data format - {ve}")
        return None
    except Exception as e:
        logger.error(f"Failed to load configuration - {e}")
        return None
    