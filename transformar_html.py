import boto3
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime


def transform_html():
    """
    Transformar el html previamente descargardo
    y generar un csv que se guarde en un bucket S3
    """
    # Configuración de Amazon S3
    s3 = boto3.client('s3')
    bucket_name = 'zappa-bucket-julian'
    folder_prefix = 'casas_raw/'

    # Obtener enlaces de archivos HTML del bucket
    response = s3.list_objects(Bucket=bucket_name, Prefix=folder_prefix)

    # Referencias de los enlaces de las publicaciones
    href_list = []

    for obj in response['Contents']:
        key = obj['Key']

        if key.endswith('.html'):
            response = s3.get_object(Bucket=bucket_name, Key=key)
            html_content = response['Body'].read()

            soup = BeautifulSoup(html_content, 'html.parser')
            listings_cards = soup.find_all(
                'div', class_='listings__cards notSponsored')

            for card in listings_cards:
                a_tags = card.find_all('a')
                href_list.extend([a.get('href')
                                 for a in a_tags if a.get('href')])

    # Inicializar DataFrame para almacenar información de las casas
    df_casas = pd.DataFrame(
        columns=[
            'titulo',
            'habitaciones',
            'banos',
            'medio_bano',
            'metros_cuadrados',
            'precio'])

    # Iterar sobre los enlaces y extraer información
    for link in href_list:
        response = requests.get('https://casas.mitula.com.co' + link)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            title_tag = soup.find('title')
            precio = soup.find('div', class_='prices-and-fees__price')
            info = soup.find_all(
                'div',
                class_='place-details place-details--all-elements-showing'
            )

            # Extraer detalles de la casa y agregar a DataFrame
            for element in info:
                details_values = element.find_all(
                    'div', class_='details-item-value')
                especificaciones = [value.text for value in details_values]

                # Las especificaciones no tiene medio baño
                if len(especificaciones) == 3:
                    dato = [
                        {
                            'titulo': title_tag.text,
                            'habitaciones': especificaciones[0].split(' ')[0],
                            'banos': especificaciones[1].split(' ')[0],
                            'medio_bano': 0,
                            'metros_cuadrados':
                            especificaciones[2].split(' ')[0].replace(
                                '.',
                                ''),
                            'precio': precio.text.replace(
                                '.',
                                '').split(' ')[1].replace(
                                '\n',
                                '')}]
                    df_casas = df_casas._append(dato, ignore_index=True)

                # Las especificaciones contiene medio baño
                else:
                    dato = [
                        {
                            'titulo': title_tag.text,
                            'habitaciones': especificaciones[0].split(' ')[0],
                            'banos': especificaciones[1].split(' ')[0],
                            'medio_bano': especificaciones[2].split(' ')[0],
                            'metros_cuadrados':
                            especificaciones[3].split(' ')[0].replace(
                                '.',
                                ''),
                            'precio': precio.text.replace(
                                '.',
                                '').split(' ')[1].replace(
                                '\n',
                                '')}]
                    df_casas = df_casas._append(dato, ignore_index=True)

    # Convertir DataFrame a formato CSV
    csv_casas = df_casas.to_csv(index=False)
    # Obtener la fecha actual
    nombre = str(datetime.today().strftime('%Y-%m-%d'))

    # Subir archivo CSV a S3 con estructura de carpetas basada en la fecha
    s3.put_object(Body=csv_casas.replace('"', ''),
                  Bucket='zappa-bucket-julian', Key=str('casas_final/casas/' +
                  'year=' + nombre[:4] + '/month=' + nombre[5:7] +
                  '/day=' + nombre[8:] + f'/{nombre}.csv'))
