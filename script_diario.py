import os
import pandas as pd
import databricks.sql as dbsql
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# 1. Obtener variables seguras desde GitHub Secrets
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID")

REMITENTE = os.getenv("REMITENTE")
PASSWORD_APP = os.getenv("PASSWORD_APP")
DESTINATARIO = os.getenv("DESTINATARIO")

def extraer_datos_y_enviar():
    print("Conectando a Databricks...")
    connection = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=DATABRICKS_TOKEN
    )

    query = "SELECT * FROM brewdat_uc_mazana_dev.slv_maz_dataexperience_peru_adb.system_process_dev_formulario_salida" \
            " where Hora_finalizacion > '2025-01-01' " \
            "ORDER BY Hora_finalizacion desc"
    
    # Extraer a pandas
    df_formulario_salida = pd.read_sql(query, connection)
    print(f"Total registros extraídos: {len(df_formulario_salida)}")
    connection.close()

    # Guardar como Excel
    ruta_archivo = "reporte_diario.xlsx"
    df_formulario_salida.to_excel(ruta_archivo, index=False)
    
    # 2. Configurar y enviar correo
    print("Enviando correo...")
    mensaje = MIMEMultipart()
    mensaje['From'] = REMITENTE
    mensaje['To'] = DESTINATARIO
    mensaje['Subject'] = "Reporte Diario Automatizado"
    mensaje.attach(MIMEText("Hola,\n\nAdjunto encontrarás el reporte diario correspondiente a Formulario Altas - Reactivaciones.\n\nPor favor, no responder a este correo automatico.\n\nSaludos cordiales.", 'plain'))    with open(ruta_archivo, "rb") as adjunto:
        parte = MIMEBase('application', 'octet-stream')
        parte.set_payload(adjunto.read())
        encoders.encode_base64(parte)
        parte.add_header('Content-Disposition', f'attachment; filename="{ruta_archivo}"')
        mensaje.attach(parte)

    # ENVÍO CON GMAIL (Recomendado)
    servidor = smtplib.SMTP('smtp.gmail.com', 587)
    servidor.starttls()
    servidor.login(REMITENTE, PASSWORD_APP)
    servidor.sendmail(REMITENTE, DESTINATARIO, mensaje.as_string())
    servidor.quit()
    print("Proceso completado con éxito.")

if __name__ == "__main__":
    extraer_datos_y_enviar()
