import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import json

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/58.0.3029.110 Safari/537.3'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al acceder a la página web', 'details': str(e)})
        }
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Ajusta el selector según la estructura actual de la página
    table = soup.find('table')  # O usa find('table', {'class': 'nombre-de-clase'})
    if not table:
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'No se encontró la tabla en la página web'})
        }
    
    headers = [header.text.strip() for header in table.find_all('th')]
    
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')
        if len(cells) == len(headers):
            row_data = {headers[i]: cell.text.strip() for i, cell in enumerate(cells)}
            row_data['id'] = str(uuid.uuid4())
            rows.append(row_data)
        else:
            # Opcional: manejar filas con celdas faltantes o adicionales
            continue
    
    # Guardar en DynamoDB
    try:
        dynamodb = boto3.resource('dynamodb')
        dynamo_table = dynamodb.Table('TablaSismo')
        
        # Eliminar todos los elementos existentes
        scan = dynamo_table.scan()
        with dynamo_table.batch_writer() as batch:
            for each in scan.get('Items', []):
                batch.delete_item(Key={'id': each['id']})
        
        # Insertar nuevos datos
        with dynamo_table.batch_writer() as batch:
            for row in rows:
                batch.put_item(Item=row)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al interactuar con DynamoDB', 'details': str(e)})
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps(rows)
    }
