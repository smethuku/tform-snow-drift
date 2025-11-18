import logging
from components import dependencies 
from pykeepass import PyKeePass

# Initialize logger
logger = dependencies.setup_logging()
logger = logging.getLogger('app.keepass_utils')

def get_keepass_title_cred(kp_db: str, kp_key_file: str, kp_title: str):
    """
    Retrieve credentials for a specified title from a KeePass database.

    Args:
        kp_db (str): Path to the KeePass database file.
        kp_key_file (str): Path to the RSA private key file used to unlock the KeePass database.
        kp_title (str): Title of the entry to retrieve credentials for.

    Returns:
        PyKeePass entry object: The first entry matching the specified title, containing credential details.
        None: If no entry is found or an error occurs.
    """
    try:
        # Validate input parameters
        if not kp_db or not kp_key_file or not kp_title:
            logger.error("All parameters (kp_db, kp_key_file, kp_title) must be non-empty strings")
            return None

        # Initialize PyKeePass with database and key file
        kp = PyKeePass(filename=kp_db, keyfile=kp_key_file)

        # Find the first entry matching the title
        kp_cred = kp.find_entries(title=kp_title, first=True)
        
        if not kp_cred:
            logger.warning(f"No entry found for title '{kp_title}'")
            return None
            
        return kp_cred

    except FileNotFoundError as fnf_error:
        logger.error(f"File not found - {fnf_error}")
        return None
    except ValueError as ve:
        logger.error(f"Invalid input - {ve}")
        return None
    except Exception as e:
        logger.error(f"Failed to retrieve credentials for '{kp_title}' - {e}")
        return None