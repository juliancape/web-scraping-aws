import boto3
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pandas as pd

s3 = boto3.client('s3')

bucket_name = 'zappa-bucket-julian'
folder_prefix = 'casas_raw/'

response = s3.list_objects(Bucket=bucket_name, Prefix=folder_prefix)

href_list = []
for obj in response['Contents']:
    key = obj['Key']
    
    # Verificar si el archivo tiene extensión .html
    if key.endswith('.html'):
        # Descargar el contenido del archivo desde S3
        response = s3.get_object(Bucket=bucket_name, Key=key)
        html_content = response['Body'].read()

        
        soup = BeautifulSoup(html_content, 'html.parser')
        # Encontrar todos los elementos con la clase 'listings__cards notSponsored'
        listings_cards = soup.find_all('div', class_='listings__cards notSponsored')

        for card in listings_cards:
            # Encontrar todas las etiquetas <a> dentro de cada elemento
            a_tags = card.find_all('a')
            
            # Obtener las referencias href y agregarlas a la lista
            href_list.extend([a.get('href') for a in a_tags if a.get('href')])
    
    pass

x = 0
df_casas = pd.DataFrame(columns=['titulo', 'habitaciones', 'baños', 'medio_baño', 'metros_cuadrados', 'precio'])
for link in href_list:
    print(link)
    response = requests.get('https://casas.mitula.com.co'+link)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        title_tag = soup.find('title')
        precio = soup.find('div', class_='prices-and-fees__price')
        info = soup.find_all('div', class_='place-details place-details--all-elements-showing')
        print(title_tag.text, precio.text)
        for element in info:
            details_values = element.find_all('div', class_='details-item-value')
            
            especificaciones = []
            for value in details_values:
                especificaciones.append(value.text)
            print('XX', especificaciones)
            
            if len(especificaciones) == 3:
                dato = [{'titulo': title_tag.text, 'habitaciones': especificaciones[0].split(' ')[0], 
                          'baños': especificaciones[1].split(' ')[0], 
                          'medio_baño': 0, 
                          'metros_cuadrados': especificaciones[2].split(' ')[0].replace('.', ''), 
                          'precio': precio.text.replace('.', '').split(' ')[1].replace('\n', '')}]
                df_casas = df_casas._append(dato, ignore_index=True)
            else:
                dato = [{'titulo': title_tag.text, 'habitaciones': especificaciones[0].split(' ')[0], 
                          'baños': especificaciones[1].split(' ')[0], 
                          'medio_baño': especificaciones[2].split(' ')[0], 
                          'metros_cuadrados': especificaciones[3].split(' ')[0].replace('.', ''), 
                          'precio': precio.text.replace('.', '').split(' ')[1].replace('\n', '')}]
                df_casas = df_casas._append(dato, ignore_index=True)
    x += 1
    if x == 5:
        break

csv_casas = df_casas.to_csv( index=False)
nombre = str(datetime.today().strftime('%Y-%m-%d'))
boto3.client('s3').put_object(Body=csv_casas,Bucket='zappa-bucket-julian',
                              Key=str('casas_final/casas/' +'year=' +nombre[:4]+'/month=' +nombre[5:7]+'/day=' +nombre[8:]+f'/{nombre}.csv'))