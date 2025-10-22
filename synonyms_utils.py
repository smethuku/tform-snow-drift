import json
import logging
from typing import Dict
from dependencies import setup_logging

# Initialize logger
logger = setup_logging()
logger = logging.getLogger('app.synonyms_utils')

def load_synonyms(synonyms_config: str) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Load and parse synonyms mapping from a JSON configuration file.

    Args:
        synonyms_config (str): Path to the JSON file containing synonyms mapping.

    Returns:
        Dict[str, Dict[str, Dict[str, str]]]: A nested dictionary containing the synonyms mapping.

    Raises:
        ValueError: If the synonyms_config path is empty or not a string.
        FileNotFoundError: If the specified configuration file does not exist.
        json.JSONDecodeError: If the file contains invalid JSON.
        ValueError: If the loaded data is not a dictionary.
        RuntimeError: For other unexpected errors during file reading or parsing.
    """
    try:
        # Validate input parameter
        if not isinstance(synonyms_config, str) or not synonyms_config.strip():
            raise ValueError("synonyms_config must be a non-empty string")

        # Open and read the JSON configuration file
        with open(synonyms_config, "r") as f:
            synonyms_data = json.load(f)

        # Verify that the loaded data is a dictionary
        if not isinstance(synonyms_data, dict):
            raise ValueError("Synonyms configuration file must contain a dictionary")

        return synonyms_data

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
        logger.error(f"Failed to load synonyms configuration - {e}")
        raise RuntimeError(f"Failed to load synonyms configuration: {e}")
