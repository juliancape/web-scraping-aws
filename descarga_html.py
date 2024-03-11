import requests
import boto3
from datetime import datetime


def get_html():
    """
    Obtener las 5 primeras paginas de la url
    especificada y guardarla en un bucket S3
    """
    for npage in range(1, 6):
        url = f"""https://casas.mitula.com.co/searchRE/
        orden-0/op-1/q-bogot%C3%A1/pag-{npage}?req_sgmt
        =REVTS1RPUDtVU0VSX1NFQVJDSDtTRVJQOw=="""

        # Realizar la solicitud GET a la URL
        response = requests.get(url)
        # Verificar si la solicitud fue exitosa (código de estado 200)
        if response.status_code == 200:
            # Configurar cliente de S3
            s3_client = boto3.client("s3")
            # Obtener fecha actual
            current_date = datetime.now().strftime("%Y-%m-%d")

            # Definir la ruta en S3
            s3_path = f"casas_raw/contenido-pag-{npage}-{current_date}.html"
            # Subir contenido a S3
            s3_client.put_object(Bucket="zappa-bucket-julian",
                                 Key=s3_path,
                                 Body=response.text)
