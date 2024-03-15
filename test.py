import requests
from transformar_html import transform_html

def test_real_request_status_code():
    url = "https://casas.mitula.com.co/searchRE/orden-0/op-1/q-bogot%C3%A1/pag-1?req_sgmt=REVTS1RPUDtVU0VSX1NFQVJDSDtTRVJQOw=="
    
    # Realizar la solicitud GET a la URL
    response = requests.get(url)
    
    # Verificar que el código de estado sea 200
    assert response.status_code == 200

def test_transform():
    dict_data = {
        'href': '/adform/24301-256-12ee-3db4fbfdffc4-863d-df402300-c603?page=2&pos=0&t_sec=26&t_or=1&t_pvid=4e01e911-98a4-4f57-a284-98f0ec0ff05b', 
        'price': '440,000,000', 
        'currency': 'COP', 
        'operation_type': 'SALE', 
        'rooms': '3', 
        'location': 'Bogotá, D.C., Bogotá, D.C.', 
        'floor_area': '59 m²', 
        'viewed': 'TODO'}
    assert dict_data == transform_html(test = True)