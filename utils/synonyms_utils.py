import json
import logging
from typing import Dict
from components import dependencies 

# Initialize logger
logger = dependencies.setup_logging()
logger = logging.getLogger('app.synonyms_utils')

def load_synonyms(synonyms_config: str) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Load and parse synonyms mapping from a JSON configuration file.

    Args:
        synonyms_config (str): Path to the JSON file containing synonyms mapping.

    Returns:
        Dict[str, Dict[str, Dict[str, str]]]: A nested dictionary containing the synonyms mapping.
    """
    try:
        # Validate input parameter
        if not isinstance(synonyms_config, str) or not synonyms_config.strip():
            logger.error("synonyms_config must be a non-empty string")
            return None

        # Open and read the JSON configuration file
        with open(synonyms_config, "r") as f:
            synonyms_data = json.load(f)

        # Verify that the loaded data is a dictionary
        if not isinstance(synonyms_data, dict):
            logger.error("Synonyms configuration file must contain a dictionary")
            return None

        return synonyms_data

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
        logger.error(f"Failed to load synonyms configuration - {e}")
        return None
