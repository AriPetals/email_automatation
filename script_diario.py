import os
import pandas as pd
import databricks.sql as dbsql
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime  # <-- Este import es el que faltaba para que no de error

# 1. Obtener variables seguras desde GitHub Secrets
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID")

REMITENTE = os.getenv("REMITENTE")
PASSWORD_APP = os.getenv("PASSWORD_APP")
DESTINATARIO = os.getenv("DESTINATARIO") # Ej: "correo1@test.com,correo2@test.com"

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
    
    # Extraer a pandas (usamos cursor para evitar el Warning amarillo)
    with connection.cursor() as cursor:
        cursor.execute(query)
        datos = cursor.fetchall()
        # Obtener los nombres de las columnas
        columnas = [desc[0] for desc in cursor.description]
        df_formulario_salida = pd.DataFrame(datos, columns=columnas)
        
    print(f"Total registros extraídos: {len(df_formulario_salida)}")
    connection.close()

    # Obtener fecha actual en formato ddmmyyyy
    fecha_actual = datetime.now().strftime("%d%m%Y")

    # Guardar como Excel
    ruta_archivo = f"formulario_salida ({fecha_actual}).xlsx"
    df_formulario_salida.to_excel(ruta_archivo, index=False)
    
    # Convertir el string de destinatarios separados por comas en una lista
    lista_destinatarios = DESTINATARIO.split(',')
    
    # 2. Configurar y enviar correo
    print("Enviando correo...")
    mensaje = MIMEMultipart()
    mensaje['From'] = REMITENTE
    mensaje['To'] = DESTINATARIO 
    mensaje['Subject'] = f"Reporte Diario Automatizado ({fecha_actual})"
    mensaje.attach(MIMEText("Hola,\n\nAdjunto encontrarás el reporte diario correspondiente a Formulario Altas - Reactivaciones.\n\nPor favor, no responder a este correo automático.\n\nSaludos cordiales.", 'plain'))

    with open(ruta_archivo, "rb") as adjunto:
        parte = MIMEBase('application', 'octet-stream')
        parte.set_payload(adjunto.read())
        encoders.encode_base64(parte)
        parte.add_header('Content-Disposition', f'attachment; filename="{ruta_archivo}"')
        mensaje.attach(parte)

    # ENVÍO CON GMAIL
    servidor = smtplib.SMTP('smtp.gmail.com', 587)
    servidor.starttls()
    servidor.login(REMITENTE, PASSWORD_APP)
    servidor.sendmail(REMITENTE, lista_destinatarios, mensaje.as_string())
    servidor.quit()
    print(f"Proceso completado. Correo enviado a: {DESTINATARIO}")

if __name__ == "__main__":
    extraer_datos_y_enviar()
