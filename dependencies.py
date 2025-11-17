# dependencies.py
import importlib.util
import subprocess
import sys
import logging
from pathlib import Path
from typing import Dict, Any

# ----------------------------------------------------------------------
# 1. Logging configuration (same timestamped file you already use)
# ----------------------------------------------------------------------
def setup_logging(log_dir: str = "logs") -> logging.Logger:
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    timestamp = Path(__file__).stem  # fallback if called before main
    try:
        # Try to reuse the timestamp from main_workflow if it already exists
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    except Exception:
        pass

    log_file = log_path / f"tf_snow_drift_{timestamp}.log"

    logger = logging.getLogger('app')
    logger.setLevel(logging.INFO)

    if not logger.handlers:                     # avoid duplicate handlers
        # Console
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
        logger.addHandler(ch)

        # File
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
        logger.addHandler(fh)

    return logger


logger = setup_logging()


# ----------------------------------------------------------------------
# 2. Install missing packages from requirements.txt
# ----------------------------------------------------------------------
def install_requirements(req_file: Path = Path("requirements.txt")) -> None:
    """
    Read ``requirements.txt`` and ``pip install`` any package that is not importable.
    """
    if not req_file.is_file():
        logger.error(f"requirements.txt not found at {req_file.resolve()}")
        raise FileNotFoundError(f"requirements.txt missing: {req_file}")

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
    else:
        logger.info("All required packages are already available.")


# ----------------------------------------------------------------------
# 3. Original version-check (now optional – runs after install)
# ----------------------------------------------------------------------
DEPENDENCIES = {
    "hvac": {"min_version": "1.0.0", "purpose": "HashiCorp Vault interactions"},
    "snowflake.connector": {"min_version": "2.7.0", "purpose": "Snowflake database queries"},
    "pykeepass": {"min_version": "4.0.0", "purpose": "KeePass database access"},
    "requests": {"min_version": "2.25.0", "purpose": "HTTP requests for Terraform Cloud API"},
    "cryptography": {"min_version": "3.4.0", "purpose": "Private key handling for Snowflake"},
    # smtplib is stdlib → no version check
}

def check_dependencies() -> Dict[str, Any]:
    """
    Verify that all required libraries are importable and meet minimum versions.
    Returns a status dictionary; raises RuntimeError only on **critical** failures.
    """
    status_dict: Dict[str, Any] = {}
    critical = False

    for mod, info in DEPENDENCIES.items():
        entry = {
            "installed": False,
            "version": None,
            "min_version": info["min_version"],
            "error": None,
        }

        spec = importlib.util.find_spec(mod)
        if spec is None:
            entry["error"] = f"Module {mod} not found. Required for {info['purpose']}."
            critical = True
        else:
            entry["installed"] = True
            try:
                module = importlib.import_module(mod)
                if info["min_version"]:
                    ver = getattr(module, "__version__", None)
                    entry["version"] = ver or "unknown"
                    if ver and ver < info["min_version"]:
                        entry["error"] = (
                            f"{mod} version {ver} < required {info['min_version']}"
                        )
                        critical = True
            except Exception as exc:
                entry["error"] = f"Error inspecting {mod}: {exc}"
                critical = True

        status_dict[mod] = entry

    # ----- logging -----
    for mod, st in status_dict.items():
        if st["error"]:
            logger.error(st["error"])
        else:
            logger.info(f"{mod}: installed (v{st['version']})")

    if critical:
        raise RuntimeError("Critical dependency issues detected – see log for details.")

    return status_dict


# ----------------------------------------------------------------------
# 4. Public helper called from main_workflow.py
# ----------------------------------------------------------------------
def ensure_dependencies(req_file: Path = Path("requirements.txt")) -> None:
    """
    One-stop function:
      1. Install anything missing from requirements.txt
      2. Run the version-check safety net
    """
    logger.info("Ensuring all Python dependencies are satisfied...")
    install_requirements(req_file)
    check_dependencies()
    logger.info("Dependency check completed successfully.")


# ----------------------------------------------------------------------
# 5. Environment setup (directories + deps)
# ----------------------------------------------------------------------
def setup_environment(alerts_location: str, log_dir: str = "logs") -> None:
    """
    Create needed folders and guarantee dependencies.
    """
    alerts_path = Path(alerts_location).resolve()
    if not alerts_path.exists():
        logger.info(f"Creating alerts directory: {alerts_path}")
        alerts_path.mkdir(parents=True, exist_ok=True)

    (alerts_path / "TerraformStateFile").mkdir(parents=True, exist_ok=True)
    (alerts_path / "Drift_Output").mkdir(parents=True, exist_ok=True)

    # Ensure deps *after* folders exist (helps pip write cache)
    ensure_dependencies()