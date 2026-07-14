"""
Email service for sending notebook URLs
"""
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

from .config import settings

logger = logging.getLogger(__name__)


def send_verification_code_email(email: str, code: str) -> bool:
    """Send a login verification code (OTP) to the user via email."""
    if not settings.SMTP_HOST or not settings.SMTP_USER:
        logger.warning("SMTP not configured, cannot send verification code")
        return False

    ttl_minutes = max(1, int(settings.EMAIL_OTP_TTL_SECONDS) // 60)
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'Your Radeon Cloud login code'
        msg['From'] = settings.SMTP_FROM
        msg['To'] = email

        text_content = (
            f"Your Radeon Cloud verification code is: {code}\n\n"
            f"It is valid for {ttl_minutes} minutes. If you did not request this, ignore this email.\n"
        )
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 520px; margin: 0 auto; padding: 20px; }}
        .header {{ background: linear-gradient(135deg, #ed1c24, #000); color: white; padding: 18px; text-align: center; border-radius: 8px 8px 0 0; }}
        .content {{ padding: 24px; background: #f9f9f9; border-radius: 0 0 8px 8px; }}
        .code {{ font-size: 32px; font-weight: 700; letter-spacing: 8px; color: #ed1c24; text-align: center; margin: 16px 0; }}
        .footer {{ padding: 16px; text-align: center; color: #666; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header"><h2 style="margin:0;">AMD Radeon Cloud</h2></div>
        <div class="content">
            <p>Use the following verification code to sign in:</p>
            <div class="code">{code}</div>
            <p>This code is valid for {ttl_minutes} minutes. If you did not request it, you can safely ignore this email.</p>
        </div>
        <div class="footer"><p>AMD Radeon Cloud</p></div>
    </div>
</body>
</html>
"""
        msg.attach(MIMEText(text_content, 'plain'))
        msg.attach(MIMEText(html_content, 'html'))

        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=20) as server:
            server.starttls()
            server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
            server.send_message(msg)

        logger.info("Sent login verification code to %s", email)
        return True
    except Exception as e:
        logger.error("Failed to send verification code to %s: %s", email, e)
        return False


def send_notebook_url_email(email: str, notebook_url: str) -> bool:
    """Send notebook URL to user via email"""
    
    if not settings.SMTP_HOST or not settings.SMTP_USER:
        logger.warning("SMTP not configured, skipping email send")
        return False
    
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'Your AMD OneClick Notebook is Ready'
        msg['From'] = settings.SMTP_FROM
        msg['To'] = email
        
        text_content = f"""
Your AMD OneClick Notebook is Ready!

Access your Jupyter Notebook at:
{notebook_url}

Note: This notebook instance will be automatically destroyed after {settings.MAX_LIFETIME_HOURS} hours 
or after {settings.IDLE_TIMEOUT_MINUTES} minutes of inactivity.

Happy coding!
AMD OneClick Team
"""
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: linear-gradient(135deg, #ed1c24, #000); color: white; padding: 20px; text-align: center; }}
        .content {{ padding: 20px; background: #f9f9f9; }}
        .button {{ display: inline-block; padding: 12px 24px; background: #ed1c24; color: white; 
                   text-decoration: none; border-radius: 4px; margin: 20px 0; }}
        .footer {{ padding: 20px; text-align: center; color: #666; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Your Notebook is Ready!</h1>
        </div>
        <div class="content">
            <p>Hi there!</p>
            <p>Your AMD OneClick Jupyter Notebook has been created and is ready to use.</p>
            <p style="text-align: center;">
                <a href="{notebook_url}" class="button">Open Notebook</a>
            </p>
            <p><strong>Direct URL:</strong><br>
            <a href="{notebook_url}">{notebook_url}</a></p>
            <p><strong>Important:</strong></p>
            <ul>
                <li>Maximum session time: {settings.MAX_LIFETIME_HOURS} hours</li>
                <li>Auto-shutdown after {settings.IDLE_TIMEOUT_MINUTES} minutes of inactivity</li>
            </ul>
        </div>
        <div class="footer">
            <p>AMD OneClick Notebook Manager</p>
        </div>
    </div>
</body>
</html>
"""
        
        msg.attach(MIMEText(text_content, 'plain'))
        msg.attach(MIMEText(html_content, 'html'))
        
        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
            server.starttls()
            server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
            server.send_message(msg)
        
        logger.info(f"Sent notebook URL email to {email}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email to {email}: {e}")
        return False
