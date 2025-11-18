import logging
import smtplib
from email.mime.text import MIMEText
from typing import Dict, Any, List
from components import dependencies

# Initialize logger
logger = dependencies.setup_logging()
logger = logging.getLogger('app.mail_utils')

def send_email(subject: str, message: str, sender_email: str, recipients: str | List[str]) -> bool:
    """
    Send an email notification with the specified subject and message.

    Args:
        subject (str): Email subject.
        message (str): Email body.
        sender_email (str): Sender's email address.
        recipients (str | List[str]): Recipient email address(es).
    """
    try:
        if isinstance(recipients, str):
            recipients = [recipients]
        if not all(isinstance(r, str) and r.strip() for r in recipients):
            logger.error("All recipients must be non-empty strings")
            return None

        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipients)

        # Use a placeholder SMTP server configuration (update with your SMTP details)
        with smtplib.SMTP('mailrelay.dimensional.com') as server:
            server.sendmail(sender_email,[recipients], msg.as_string())

        logger.info(f"Email sent successfully: {subject}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False
        