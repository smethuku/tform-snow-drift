import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from pathlib import Path
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
            server.sendmail(sender_email, recipients, msg.as_string())

        logger.info(f"Email sent successfully: {subject}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def send_drift_email(subject: str, account_name: str, drift_output: Dict[str, List[Dict[str, Any]]], 
                     output_file: str, sender_email: str, recipients: str | List[str]) -> bool:
    """
    Send an HTML email notification with a table showing drift-detected resource types
    and attach the drift JSON file.

    Args:
        subject (str): Email subject.
        account_name (str): Name of the Snowflake account.
        drift_output (Dict[str, List[Dict[str, Any]]]): Dictionary with resource types as keys and drift lists as values.
        output_file (str): Path to the drift output JSON file.
        sender_email (str): Sender's email address.
        recipients (str | List[str]): Recipient email address(es).
    
    Returns:
        bool: True if email sent successfully, False otherwise.
    """
    try:
        if isinstance(recipients, str):
            recipients = [recipients]
        if not all(isinstance(r, str) and r.strip() for r in recipients):
            logger.error("All recipients must be non-empty strings")
            return False

        # Extract resource types with drifts
        resource_types_with_drift = [resource_type for resource_type, drifts in drift_output.items() if drifts]
        
        if not resource_types_with_drift:
            logger.warning("No drift detected, skipping email")
            return True

        # Count total drifts
        total_drifts = sum(len(drifts) for drifts in drift_output.values())

        # Create HTML email body
        html_body = f"""
        <html>
            <head>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        line-height: 1.6;
                        color: #333;
                    }}
                    .container {{
                        max-width: 600px;
                        margin: 0 auto;
                        padding: 20px;
                    }}
                    table {{
                        border-collapse: collapse;
                        margin: 20px 0;
                    }}
                    th {{
                        background-color: #f0f0f0;
                        color: #333;
                        padding: 10px;
                        text-align: left;
                        font-weight: bold;
                        border: 1px solid #ddd;
                    }}
                    td {{
                        padding: 10px;
                        border: 1px solid #ddd;
                    }}
                    .attachment-note {{
                        background-color: #e8f5e9;
                        padding: 10px;
                        border-radius: 5px;
                        margin-top: 20px;
                        border-left: 4px solid #4caf50;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <p><strong>Account:</strong> {account_name}</p>
                    <p><strong>Total Drifts:</strong> {total_drifts}</p>
                    
                    <p><strong>Drift Detected in the Following Resource Types:</strong></p>
                    <table>
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>Resource Type</th>
                                <th>Drift Count</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        
        # Add table rows for each resource type
        for idx, resource_type in enumerate(sorted(resource_types_with_drift), start=1):
            drift_count = len(drift_output[resource_type])
            html_body += f"""
                            <tr>
                                <td>{idx}</td>
                                <td>{resource_type}</td>
                                <td>{drift_count}</td>
                            </tr>
            """
        
        html_body += """
                        </tbody>
                    </table>
                    
                    <div class="attachment-note">
                        <p><strong>📎 Note:</strong> The drift report JSON file is attached to this email.</p>
                    </div>
                </div>
            </body>
        </html>
        """

        # Create plain text version as fallback
        plain_text = f"""Account: {account_name}
Total Drifts: {total_drifts}

Drift Detected in the Following Resource Types:
"""
        for idx, resource_type in enumerate(sorted(resource_types_with_drift), start=1):
            drift_count = len(drift_output[resource_type])
            plain_text += f"{idx}. {resource_type} - {drift_count} drift(s)\n"
        
        plain_text += f"\n📎 Note: The drift report JSON file is attached to this email."

        # Create multipart message
        msg = MIMEMultipart('mixed')
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipients)

        # Create alternative part for text and HTML
        msg_alternative = MIMEMultipart('alternative')
        
        # Attach both plain text and HTML versions
        part1 = MIMEText(plain_text, 'plain')
        part2 = MIMEText(html_body, 'html')
        
        msg_alternative.attach(part1)
        msg_alternative.attach(part2)
        msg.attach(msg_alternative)

        # Attach the JSON file
        try:
            output_path = Path(output_file)
            if output_path.exists() and output_path.is_file():
                with open(output_file, 'rb') as f:
                    attachment = MIMEApplication(f.read(), _subtype='json')
                    attachment.add_header(
                        'Content-Disposition', 
                        'attachment', 
                        filename=output_path.name
                    )
                    msg.attach(attachment)
                logger.info(f"Attached drift file: {output_path.name}")
            else:
                logger.warning(f"Output file not found for attachment: {output_file}")
        except Exception as attach_error:
            logger.error(f"Failed to attach drift file: {attach_error}")
            # Continue sending email even if attachment fails

        # Send email
        with smtplib.SMTP('mailrelay.dimensional.com') as server:
            server.sendmail(sender_email, recipients, msg.as_string())

        logger.info(f"Drift email sent successfully with attachment: {subject}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send drift email: {e}")
        return False


def send_consolidated_drift_email(
    account_drift_summary: List[Dict[str, Any]], 
    sender_email: str, 
    recipients: str | List[str]
) -> bool:
    """
    Send a consolidated HTML email notification summarizing drift across all accounts
    with all drift JSON files attached.

    Args:
        account_drift_summary (List[Dict[str, Any]]): List of dictionaries containing drift summary per account.
            Each dict should have: 'account_name', 'total_drifts', 'resource_types', 'output_file'
        sender_email (str): Sender's email address.
        recipients (str | List[str]): Recipient email address(es).
    
    Returns:
        bool: True if email sent successfully, False otherwise.
    """
    try:
        if isinstance(recipients, str):
            recipients = [recipients]
        if not all(isinstance(r, str) and r.strip() for r in recipients):
            logger.error("All recipients must be non-empty strings")
            return False

        # Filter accounts with drifts
        accounts_with_drift = [acc for acc in account_drift_summary if acc['total_drifts'] > 0]
        
        if not accounts_with_drift:
            logger.info("No drift detected across all accounts, skipping consolidated email")
            return True

        # Calculate totals
        total_accounts_with_drift = len(accounts_with_drift)
        grand_total_drifts = sum(acc['total_drifts'] for acc in accounts_with_drift)

        # Create HTML email body
        html_body = f"""
        <html>
            <head>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        line-height: 1.6;
                        color: #333;
                    }}
                    .container {{
                        max-width: 800px;
                        margin: 0 auto;
                        padding: 20px;
                    }}
                    .summary {{
                        background-color: #f9f9f9;
                        padding: 15px;
                        border-radius: 5px;
                        margin-bottom: 20px;
                        border-left: 4px solid #666;
                    }}
                    table {{
                        border-collapse: collapse;
                        margin: 20px 0;
                    }}
                    th {{
                        background-color: #f0f0f0;
                        color: #333;
                        padding: 10px;
                        text-align: left;
                        font-weight: bold;
                        border: 1px solid #ddd;
                    }}
                    td {{
                        padding: 10px;
                        border: 1px solid #ddd;
                    }}
                    .account-section {{
                        margin-top: 30px;
                        padding-top: 20px;
                        border-top: 2px solid #ddd;
                    }}
                    .attachment-note {{
                        background-color: #e8f5e9;
                        padding: 10px;
                        border-radius: 5px;
                        margin-top: 20px;
                        border-left: 4px solid #4caf50;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h2>Snowflake-Terraform Drift Detection Summary</h2>
                    
                    <div class="summary">
                        <p><strong>Total Accounts with Drift:</strong> {total_accounts_with_drift}</p>
                        <p><strong>Grand Total Drifts:</strong> {grand_total_drifts}</p>
                    </div>
        """

        # Add section for each account
        for account in accounts_with_drift:
            account_name = account['account_name']
            total_drifts = account['total_drifts']
            resource_types = account['resource_types']
            
            html_body += f"""
                    <div class="account-section">
                        <h3>Account: {account_name}</h3>
                        <p><strong>Total Drifts:</strong> {total_drifts}</p>
                        
                        <p><strong>Drift Detected in the Following Resource Types:</strong></p>
                        <table>
                            <thead>
                                <tr>
                                    <th>#</th>
                                    <th>Resource Type</th>
                                    <th>Drift Count</th>
                                </tr>
                            </thead>
                            <tbody>
            """
            
            # Add rows for each resource type in this account
            for idx, (resource_type, drift_count) in enumerate(sorted(resource_types.items()), start=1):
                html_body += f"""
                                <tr>
                                    <td>{idx}</td>
                                    <td>{resource_type}</td>
                                    <td>{drift_count}</td>
                                </tr>
                """
            
            html_body += """
                            </tbody>
                        </table>
                    </div>
            """

        html_body += f"""
                    <div class="attachment-note">
                        <p><strong>📎 Attachments:</strong> All {total_accounts_with_drift} drift report JSON file(s) are attached to this email.</p>
                    </div>
                </div>
            </body>
        </html>
        """

        # Create plain text version as fallback
        plain_text = f"""Snowflake-Terraform Drift Detection Summary
{'=' * 50}

Total Accounts with Drift: {total_accounts_with_drift}
Grand Total Drifts: {grand_total_drifts}

"""
        
        for account in accounts_with_drift:
            account_name = account['account_name']
            total_drifts = account['total_drifts']
            resource_types = account['resource_types']
            
            plain_text += f"""
Account: {account_name}
Total Drifts: {total_drifts}

Drift Detected in the Following Resource Types:
"""
            for idx, (resource_type, drift_count) in enumerate(sorted(resource_types.items()), start=1):
                plain_text += f"{idx}. {resource_type} - {drift_count} drift(s)\n"
            
            plain_text += "-" * 50 + "\n"

        plain_text += f"\n📎 Attachments: All {total_accounts_with_drift} drift report JSON file(s) are attached to this email."

        # Create multipart message
        msg = MIMEMultipart('mixed')
        msg['Subject'] = "Snowflake-Terraform Drift Detection - Consolidated Report"
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipients)

        # Create alternative part for text and HTML
        msg_alternative = MIMEMultipart('alternative')
        
        # Attach both plain text and HTML versions
        part1 = MIMEText(plain_text, 'plain')
        part2 = MIMEText(html_body, 'html')
        
        msg_alternative.attach(part1)
        msg_alternative.attach(part2)
        msg.attach(msg_alternative)

        # Attach all JSON files from accounts with drift
        attached_count = 0
        for account in accounts_with_drift:
            output_file = account.get('output_file')
            if output_file:
                try:
                    output_path = Path(output_file)
                    if output_path.exists() and output_path.is_file():
                        with open(output_file, 'rb') as f:
                            attachment = MIMEApplication(f.read(), _subtype='json')
                            attachment.add_header(
                                'Content-Disposition', 
                                'attachment', 
                                filename=output_path.name
                            )
                            msg.attach(attachment)
                        attached_count += 1
                        logger.info(f"Attached drift file: {output_path.name}")
                    else:
                        logger.warning(f"Output file not found for attachment: {output_file}")
                except Exception as attach_error:
                    logger.error(f"Failed to attach drift file {output_file}: {attach_error}")
                    # Continue with other attachments

        # Send email
        with smtplib.SMTP('mailrelay.dimensional.com') as server:
            server.sendmail(sender_email, recipients, msg.as_string())

        logger.info(f"Consolidated drift email sent successfully with {attached_count} attachment(s) to {len(recipients)} recipient(s)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send consolidated drift email: {e}")
        return False
