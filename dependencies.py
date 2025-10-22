import importlib.util
import sys
from pathlib import Path
import logging
from datetime import datetime
from typing import Optional, Dict, Any

def setup_logging(log_dir: str = "logs") -> logging.Logger:
    """
    Configure logging for the application with console and timestamped file handlers.

    The log file is named 'tf_snow_drift_YYYYMMDD_HHMM.log' based on the current timestamp.

    Args:
        log_dir (str): Directory where log files will be stored.

    Returns:
        logging.Logger: Configured logger instance for the application.

    Raises:
        ValueError: If log_dir is invalid.
        RuntimeError: If log file directory creation fails.
    """
    try:
        # Validate input
        if not isinstance(log_dir, str) or not log_dir.strip():
            raise ValueError("log_dir must be a non-empty string")

        # Create log directory
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        # Generate timestamped log file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_file = f"tf_snow_drift_{timestamp}.log"
        log_file_path = log_path / log_file

        # Configure logger
        logger = logging.getLogger('app')
        logger.setLevel(logging.INFO)

        # Avoid adding handlers multiple times
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

            # File handler
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        return logger

    except ValueError as ve:
        raise ValueError(f"Error [setup_logging]: Invalid input - {ve}")
    except Exception as e:
        raise RuntimeError(f"Error [setup_logging]: Failed to configure logging - {e}")

# Required dependencies with minimum versions
DEPENDENCIES = {
    "hvac": {"min_version": "1.0.0", "purpose": "HashiCorp Vault interactions"},
    "snowflake.connector": {"min_version": "2.7.0", "purpose": "Snowflake database queries"},
    "pykeepass": {"min_version": "4.0.0", "purpose": "KeePass database access"},
    "requests": {"min_version": "2.25.0", "purpose": "HTTP requests for Terraform Cloud API"},
    "cryptography": {"min_version": "3.4.0", "purpose": "Private key handling for Snowflake"},
    "smtplib": {"min_version": None, "purpose": "Email notifications"},
}

def check_dependencies() -> Dict[str, Any]:
    """
    Check if required dependencies are installed and meet minimum version requirements.

    Returns:
        Dict[str, Any]: A dictionary with dependency names and status details.

    Raises:
        RuntimeError: If any critical dependency is missing or incompatible.
    """
    try:
        logger = logging.getLogger('app.dependencies')
        dependency_status = {}
        has_errors = False

        for module_name, info in DEPENDENCIES.items():
            status = {"installed": False, "version": None, "min_version": info["min_version"], "error": None}
            try:
                module_spec = importlib.util.find_spec(module_name)
                if module_spec is None:
                    status["error"] = f"Module {module_name} not found. Required for {info['purpose']}."
                    has_errors = True
                else:
                    status["installed"] = True
                    module = importlib.import_module(module_name)
                    if info["min_version"]:
                        try:
                            version = getattr(module, "__version__", None) or module.__version__
                            status["version"] = version
                            if version < info["min_version"]:
                                status["error"] = (
                                    f"Module {module_name} version {version} is below required minimum {info['min_version']}"
                                )
                                has_errors = True
                        except AttributeError:
                            status["error"] = f"Module {module_name} does not provide version information"
                            has_errors = True
                dependency_status[module_name] = status
            except Exception as e:
                status["error"] = f"Error checking module {module_name}: {e}"
                has_errors = True
                dependency_status[module_name] = status

        for module_name, status in dependency_status.items():
            if status["error"]:
                logger.error(status["error"])
            else:
                logger.info(f"Module {module_name}: Installed (version: {status['version'] or 'N/A'})")

        if has_errors:
            raise RuntimeError("One or more dependencies are missing or incompatible. Check logs for details.")

        return dependency_status

    except Exception as e:
        logger = logging.getLogger('app.dependencies')
        logger.error(f"Error [check_dependencies]: Failed to verify dependencies - {e}")
        raise RuntimeError(f"Failed to verify dependencies: {e}")

def setup_environment(alerts_location: str, log_dir: str = "logs") -> None:
    """
    Set up the environment for the application, including directory creation, logging, and dependency checks.

    Args:
        alerts_location (str): Base directory for alerts, state files, and output files.
        log_dir (str): Directory for log files.

    Raises:
        ValueError: If alerts_location or log_dir is invalid.
        RuntimeError: If dependency checks or directory creation fails.
    """
    try:
        # Setup logging
        setup_logging(log_dir)

        # Validate alerts_location
        logger = logging.getLogger('app.dependencies')
        alerts_path = Path(alerts_location).resolve()
        if not isinstance(alerts_location, str) or not alerts_location.strip():
            raise ValueError("alerts_location must be a non-empty string")
        if not alerts_path.exists():
            logger.info(f"Creating alerts directory: {alerts_path}")
            alerts_path.mkdir(parents=True, exist_ok=True)
        elif not alerts_path.is_dir():
            raise ValueError(f"alerts_location '{alerts_location}' is not a directory")

        # Create standard subdirectories
        (alerts_path / "TerraformStateFile").mkdir(parents=True, exist_ok=True)
        (alerts_path / "Drift_Output").mkdir(parents=True, exist_ok=True)

        # Check dependencies
        check_dependencies()

        logger.info("Environment setup completed successfully")

    except ValueError as ve:
        logger = logging.getLogger('app.dependencies')
        logger.error(f"Error [setup_environment]: Invalid input - {ve}")
        raise
    except Exception as e:
        logger = logging.getLogger('app.dependencies')
        logger.error(f"Error [setup_environment]: Failed to set up environment - {e}")
        raise RuntimeError(f"Failed to set up environment: {e}")