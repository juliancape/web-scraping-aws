import boto3
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime


def transform_html(test=False):
    """
    Transformar el html previamente descargardo
    y generar un csv que se guarde en un bucket S3
    """
    if test is True:
        with open("html_test.html", "r", encoding="utf-8") as file:
            html_content = file.read()
        # Parsea el contenido HTML
        soup = BeautifulSoup(html_content, "html.parser")
        card = soup.find(
            'div', class_='listing listing-card')
        data = {
            'href': card.get('data-href', ''),
            'price': card.get('data-price', ''),
            'currency': card.get('data-currency', ''),
            'operation_type': card.get('data-operation-type', ''),
            'rooms': card.get('data-rooms', ''),
            'location': card.get('data-location', ''),
            'floor_area': card.get('data-floorarea', ''),
            'viewed': card.get('data-viewed', '')
        }
        print(data)
    else:
        s3 = boto3.client('s3')
        bucket_name = 'zappa-bucket-julian-descargar'
        folder_prefix = 'casas_raw/'

        # Obtener enlaces de archivos HTML del bucket
        response = s3.list_objects(Bucket=bucket_name, Prefix=folder_prefix)
        for obj in response['Contents']:
            key = obj['Key']

            if key.endswith('.html'):
                response = s3.get_object(Bucket=bucket_name, Key=key)
                html_content = response['Body'].read()

                soup = BeautifulSoup(html_content, 'html.parser')
                # Buscar todos los elementos div con la clase 'listing
                # listing-card'
                listings_cards = soup.find_all(
                    'div', class_='listing listing-card')

                # Crear una lista para almacenar los datos
                data_list = []

                # Iterar sobre cada elemento 'listing-card'
                # deseados
                for card in listings_cards:
                    data = {
                        'href': card.get('data-href', ''),
                        'price': card.get('data-price', ''),
                        'currency': card.get('data-currency', ''),
                        'operation_type': card.get('data-operation-type', ''),
                        'rooms': card.get('data-rooms', ''),
                        'location': card.get('data-location', ''),
                        'floor_area': card.get('data-floorarea', ''),
                        'viewed': card.get('data-viewed', '')
                    }
                    data_list.append(data)

                # Crear un DataFrame de Pandas
                df = pd.DataFrame(data_list)

                # Mostrar el DataFrame
                print(df)
                # Convertir DataFrame a formato CSV
                df = df.to_csv(index=False, sep=';')
                # Obtener la fecha actual
                nombre = str(datetime.today().strftime('%Y-%m-%d'))

                # Subir archivo CSV a S3 con estructura de
                # carpetas basada en la fecha
                print(df)
                s3.put_object(Body=df.replace('"', ''),
                              Bucket='zappa-bucket-julian-transformar',
                              Key=str('casas_final/casas/' +
                                      'year=' + nombre[:4] + '/month='
                                      + nombre[5:7] +
                                      '/day=' + nombre[8:] + f'/{nombre}.csv'))
