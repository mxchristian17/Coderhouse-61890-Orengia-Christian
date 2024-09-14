import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def notify(message_type, message_extra_data = ""):

    # Messages controller
    def success():
        return f"""
        <html>
        <body>
            <h1>Proceso de ETL finalizado correctamente</h1>
            <p>Se finalizó exitosamente el proceso de ETL</p>
        </body>
        </html>
        """
    def failure():
        return f"""
        <html>
        <body>
            <h1>Error en el proceso de ETL</h1>
            <p>Ocurrió un error durante el proceso de ETL</p>
            <p>Detalles disponibles:</p>
            <p>{message_extra_data}</p>
        </body>
        </html>
        """
    def default_case():
        return f"""
        <html>
        <body>
            <h1>Algo inesperado ocurrió</h1>
            <p>Este correo se disparó por una causa inesperada. Te sugiero revisar el estado del proceso de ETL.</p>
        </body>
        </html>
        """
    def switch(case):
        switch = {
            "success": success,
            "failure": failure
        }
        return switch.get(case, default_case)()

    # Create the mail content
    mail_content = switch(message_type)

    # Set de email data
    mail_password = os.getenv('MAIL_PASSWORD')
    msg = MIMEMultipart()
    msg['From'] = os.getenv('MAIL_FROM_ADDRESS')
    msg['To'] = os.getenv('MAIL_TO_ADDRESS')
    msg['Subject'] = os.getenv('MAIL_STANDARD_SUBJECT')

    msg.attach(MIMEText(mail_content, 'html'))

    # Send the email
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(msg["From"], mail_password)
        text = msg.as_string()
        server.sendmail(msg["From"], msg["To"], text)
        server.quit()
        print(f"\033[1;32mEmail sent successfully\033[0m")
    except Exception as e:
        print(f"\033[1;31mFailed to send email: {str(e)}\033[0m")