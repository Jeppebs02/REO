import pandas as pd
import logging
import os
import smtplib
from email.mime.text import MIMEText


class EmailSender:
    def __init__(self, sender_email=None, sender_password=None):
        """Initializes the EmailSender class with sender credentials."""
        self.sender_email = sender_email or os.getenv("EMAIL_ADDRESS")
        self.sender_password = sender_password or os.getenv("EMAIL_PASSWORD")
        self.smtp_server = "smtp.gigahost.dk"
        self.smtp_port = 587  # Don't Use 465.
        logging.basicConfig(level=logging.INFO)


    def test_login(self):
        """Tests the SMTP login credentials."""
        try:
            if not self.sender_email or not self.sender_password:
                raise ValueError("Email credentials are missing. Set them via parameters or environment variables.")

            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender_email, self.sender_password)
            server.quit()

            logging.info("Successfully logged in to SMTP server.")
            return True
        except Exception as e:
            logging.error(f"Failed to log in to SMTP server, most likely connection related: {e}")
            return False

    def send_email(self, recipient_email, email_subject, email_body):
        """Sends an email using SMTP authentication."""
        try:
            if not self.sender_email or not self.sender_password:
                raise ValueError("Email credentials are missing. Set them via parameters or environment variables.")

            msg = MIMEText(email_body)
            msg["From"] = self.sender_email
            msg["To"] = recipient_email
            msg["Subject"] = email_subject

            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender_email, self.sender_password)
            server.sendmail(self.sender_email, recipient_email, msg.as_string())
            server.quit()

            logging.info(f"Email successfully sent to {recipient_email}")
            return True

        except Exception as e:
            logging.error(f"Failed to send email: {e}")
            return False

    def send_bulk_emails(self, file_path):
        """Reads a CSV file and sends personalized emails to each recipient."""
        try:
            df = pd.read_csv(file_path, delimiter=";", header=None, names=["Name", "Email"])
            recipients = df.to_records(index=False)
        except Exception as e:
            logging.error(f"Error reading CSV: {e}")
            return

        for name, email in recipients:
            email_subject = "Test email "
            email_body = f"Hej {name},\n\nDette er en test email sendt til dig og mig (jeppe).\n\nVenlig hilsen,\nREO kasserer"

            success = self.send_email(email, email_subject, email_body)

            if success:
                print(f"Email sent successfully to {name} ({email})")
            else:
                print(f"Failed to send email to {name} ({email})")