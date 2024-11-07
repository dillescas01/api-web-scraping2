import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import json
import logging

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/58.0.3029.110 Safari/537.3'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"Solicitud HTTP exitosa con status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al acceder a la página web: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al acceder a la página web', 'details': str(e)})
        }
    
    # Registrar una parte del contenido de la respuesta para verificar la presencia de la tabla
    logger.info(f"Contenido de la respuesta (primeros 500 caracteres): {response.text[:500]}")
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Ajusta el selector según la estructura actual de la página
    # Por ejemplo, si la tabla tiene una clase específica, reemplaza 'nombre-de-clase' con el valor real
    table = soup.find('table')  # O usa find('table', {'class': 'nombre-de-clase'})
    if not table:
        logger.error("No se encontró la tabla en la página web")
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'No se encontró la tabla en la página web'})
        }
    
    headers = [header.text.strip() for header in table.find_all('th')]
    logger.info(f"Encabezados encontrados: {headers}")
    
    rows = []
    for idx, row in enumerate(table.find_all('tr')[1:], start=1):  # Omitir el encabezado
        cells = row.find_all('td')
        if len(cells) != len(headers):
            logger.warning(f"Fila {idx} tiene {len(cells)} celdas, se esperaban {len(headers)}")
            continue  # O manejar según sea necesario
        
        row_data = {headers[i]: cell.text.strip() for i, cell in enumerate(cells)}
        row_data['id'] = str(uuid.uuid4())  # Agregar ID único
        rows.append(row_data)
    
    logger.info(f"Filas extraídas: {len(rows)}")
    
    # Guardar en DynamoDB
    try:
        dynamodb = boto3.resource('dynamodb')
        dynamo_table = dynamodb.Table('TablaSismo')
        
        # Eliminar todos los elementos existentes
        scan = dynamo_table.scan()
        logger.info(f"Número de elementos a eliminar: {len(scan.get('Items', []))}")
        with dynamo_table.batch_writer() as batch:
            for each in scan.get('Items', []):
                batch.delete_item(Key={'id': each['id']})
        
        logger.info("Elementos existentes eliminados correctamente.")
        
        # Insertar nuevos datos
        with dynamo_table.batch_writer() as batch:
            for row in rows:
                batch.put_item(Item=row)
        
        logger.info("Nuevos datos insertados correctamente en DynamoDB.")
    except Exception as e:
        logger.error(f"Error al interactuar con DynamoDB: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al interactuar con DynamoDB', 'details': str(e)})
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps(rows, ensure_ascii=False)
    }
