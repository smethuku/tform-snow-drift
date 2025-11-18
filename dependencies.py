import importlib.util
import sys
from pathlib import Path
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import subprocess

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

def install_requirements(req_file: Path = Path("requirements.txt")) -> None:
    """
    Read ``requirements.txt`` and ``pip install`` any package that is not importable.
    """
    try:

        logger = logging.getLogger('app.install_requirements')
        if not req_file.is_file():
            logger.error(f"requirements.txt not found at {req_file.resolve()}")
            raise FileNotFoundError(f"requirements.txt missing: {req_file}")
            return

        with req_file.open("r", encoding="utf-8") as f:
            requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

        missing = []
        for req in requirements:
            # Extract package name (before any version specifier)
            pkg_name = req.split("==")[0].split(">=")[0].split("<=")[0].split("~=")[0].strip()
            if not importlib.util.find_spec(pkg_name.replace("-", "_")):
                missing.append(req)

        if missing:
            logger.info(f"Installing {len(missing)} missing package(s): {', '.join(missing)}")
            try:
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install", *missing
                ])
                logger.info("All missing packages installed successfully.")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to install packages: {e}")
                raise RuntimeError("Dependency installation failed.") from e
                return  
        else:
            logger.info("All required packages are already available.")


    except Exception as e:
        logger = logging.getLogger('app.install_requirements')
        logger.error(f"Error [install_requirements]: Failed to install requirements - {e}")
        return



def setup_environment(alerts_location: str, log_dir: str = "logs") -> None:
    """
    Set up the environment for the application, including directory creation, logging, and dependency checks.

    Args:
        alerts_location (str): Base directory for alerts, state files, and output files.
        log_dir (str): Directory for log files.
    """
    try:
        # Setup logging
        setup_logging(log_dir)

        # Validate alerts_location
        logger = logging.getLogger('app.dependencies')
        alerts_path = Path(alerts_location).resolve()
        if not isinstance(alerts_location, str) or not alerts_location.strip():
            logger.error("alerts_location must be a non-empty string")
        if not alerts_path.exists():
            logger.info(f"Creating alerts directory: {alerts_path}")
            alerts_path.mkdir(parents=True, exist_ok=True)
        elif not alerts_path.is_dir():
            logger.error(f"alerts_location '{alerts_location}' is not a directory")

        # Create standard subdirectories
        (alerts_path / "terraformstatefiles").mkdir(parents=True, exist_ok=True)
        (alerts_path / "drift_output").mkdir(parents=True, exist_ok=True)

        # Install required libraries
        install_requirements()

        logger.info("Environment setup completed successfully")

    except ValueError as ve:
        logger = logging.getLogger('app.dependencies')
        logger.error(f"Error [setup_environment]: Invalid input - {ve}")
    except Exception as e:
        logger = logging.getLogger('app.dependencies')
        logger.error(f"Error [setup_environment]: Failed to set up environment - {e}")
        