import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from typing import List, Union
from pathlib import Path
from components import dependencies

# Initialize logger
logger = dependencies.setup_logging()
logger = logging.getLogger('app.mail_utils')


def send_email(
    subject: str,
    body: str,
    sender_email: str,
    recipients: Union[str, List[str]],
    attachment_path: Union[str, Path, None] = None,
    smtp_host: str = 'mailrelay.dimensional.com',
    smtp_port: int = 25,               # usually 25 for internal relay; change if needed
    use_tls: bool = False,             # most internal relays don't require STARTTLS
    username: str = None,              # optional - only if auth is required
    password: str = None
) -> bool:
    """
    Send an email notification, optionally with an attachment.

    Args:
        subject (str): Email subject
        body (str): Plain text email body
        sender_email (str): Sender's email address
        recipients (str | List[str]): One or more recipient email addresses
        attachment_path (str | Path | None): Optional path to file to attach
        smtp_host (str): SMTP server hostname (default: your internal relay)
        smtp_port (int): SMTP port (default: 25)
        use_tls (bool): Whether to use STARTTLS (usually False for internal relays)
        username (str): SMTP username (if authentication is required)
        password (str): SMTP password (if authentication is required)

    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    try:
        # Normalize recipients to list
        if isinstance(recipients, str):
            recipients = [r.strip() for r in recipients.split(',') if r.strip()]
        if not recipients:
            logger.error("No valid recipients provided")
            return False

        if not all(isinstance(r, str) and r.strip() for r in recipients):
            logger.error("All recipients must be non-empty strings")
            return False

        # Create multipart message (needed even without attachment)
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipients)

        # Attach body as plain text
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        # Add attachment if provided
        if attachment_path:
            attachment_path = Path(attachment_path)
            if not attachment_path.is_file():
                logger.warning(f"Attachment not found or not a file: {attachment_path}")
            else:
                try:
                    with open(attachment_path, 'rb') as f:
                        part = MIMEApplication(
                            f.read(),
                            Name=attachment_path.name
                        )
                    part['Content-Disposition'] = f'attachment; filename="{attachment_path.name}"'
                    msg.attach(part)
                    logger.debug(f"Attached file: {attachment_path.name}")
                except Exception as attach_err:
                    logger.warning(f"Failed to attach {attachment_path}: {attach_err}")

        # Connect and send
        smtp_args = (smtp_host, smtp_port)

        with smtplib.SMTP(*smtp_args, timeout=30) as server:
            if use_tls:
                server.starttls()

            if username and password:
                server.login(username, password)

            server.sendmail(
                from_addr=sender_email,
                to_addrs=recipients,
                msg=msg.as_string()
            )

        logger.info(f"Email sent successfully → Subject: {subject!r} | To: {', '.join(recipients)}")
        if attachment_path:
            logger.info(f"   with attachment: {Path(attachment_path).name}")
        return True

    except smtplib.SMTPException as smtp_err:
        logger.error(f"SMTP error while sending email: {smtp_err}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending email: {e}", exc_info=True)
        return False
