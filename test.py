import requests

def test_real_request_status_code():
    url = "https://casas.mitula.com.co/searchRE/orden-0/op-1/q-bogot%C3%A1/pag-1?req_sgmt=REVTS1RPUDtVU0VSX1NFQVJDSDtTRVJQOw=="
    
    # Realizar la solicitud GET a la URL
    response = requests.get(url)
    
    # Verificar que el código de estado sea 200
    assert response.status_code == 200
