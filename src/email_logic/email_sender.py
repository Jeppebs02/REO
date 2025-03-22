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

            # Specify MIME type as HTML
            msg = MIMEText(email_body, "html")
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
        successcounter = 0
        failcounter = 0
        try:
            df = pd.read_csv(
                file_path,
                delimiter=";",
                usecols=[0, 1],
                names=["Name", "Email"],
                header=None,
                skip_blank_lines=True,
                dtype=str,
                encoding = "latin1"  # Fixes Danish special characters
            )
            df["Name"] = df["Name"].str.strip()
            df["Email"] = df["Email"].str.strip()
            df.dropna(subset=["Name", "Email"], inplace=True)

            recipients = df.to_records(index=False)

        except Exception as e:
            logging.error(f"Error reading CSV: {e}")
            return

        for name, email in recipients:
            email_subject = ("Rykker – Kontingentbetaling til REO for 2025")

            email_body = f"""\
            <html>
              <body style="font-family: Arial, sans-serif; font-size: 16px; line-height: 1.5; color: #000;">
                <p>Kære {name},</p>

                <p>Vi kan se, at vi endnu ikke har modtaget din betaling af kontingentet for 2025.</p>

                <p>Hvis denne mail har krydset din indbetaling, skal du naturligvis blot se bort fra denne rykker – og tak for støtten!</p>

                <p>Som tidligere nævnt koster et medlemskab:</p>
                <ul>
                  <li>300 kr. for enkeltpersoner</li>
                  <li>400 kr. for ægtepar</li>
                  <li>50 kr. for unge under uddannelse</li>
                  <li>1.500 kr. for firmamedlemskab</li>
                </ul>

                <p><strong>Du kan betale via:</strong><br>
                <strong>Netbank:</strong> Danske Bank – Reg.nr. 9570 Konto: 3000753<br>
                <strong>MobilePay:</strong> 20009</p>

                <p>Frivillige bidrag modtages også meget gerne via bankkontoen.</p>

                <p>Hvis du har spørgsmål til indbetalingen, er du meget velkommen til at kontakte undertegnede.</p>

                <p>Vi håber meget, at du fortsat vil støtte op om REO’s arbejde – og ikke mindst, at vi ses til generalforsamlingen lørdag den 29. marts.</p>

                <p><strong>Se invitationen her:</strong><br>
                <a href="https://reo.dk/wp-content/uploads/Indkaldelse_til_REO_generalforsamling_2025.pdf" target="_blank">
                  https://reo.dk/wp-content/uploads/Indkaldelse_til_REO_generalforsamling_2025.pdf
                </a></p>

                <p style="color: red;"><strong>Vær opmærksom på, at stemmeret på generalforsamlingen kræver, at kontingentet for 2025 er betalt.</strong></p>

                <p>På forhånd tak!</p>

                <p>Med venlig hilsen<br>
                Søren Søndergaard <br>
                Kasserer i REO</p>
              </body>
            </html>
            """

            success = self.send_email(email, email_subject, email_body)

            if success:
                print(f"Email sent successfully to {name} ({email})")
                successcounter = successcounter + 1
            else:
                print(f"Failed to send email to {name} ({email})")
                failcounter = failcounter + 1

        print(f"Failed emails: {failcounter}\n")
        print(f"Successful emails: {successcounter}")
