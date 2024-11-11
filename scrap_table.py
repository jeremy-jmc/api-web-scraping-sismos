import requests
import boto3
import uuid
import datetime


def lambda_handler(event, context):
    try:
        # Webscrapping
        url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2024"
        response = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*'
        })
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'body': 'Error al acceder a la página web'
            }

        # Data parsing and processing
        data = response.json()
        for record in data:
            record['descargas'] = record.pop('reporte_acelerometrico_pdf')

        # for record in data:
        #     record['fecha_local'] = datetime.datetime.strptime(record['fecha_local'], '%Y-%m-%dT%H:%M:%S.%fZ')
        #     record['hora_local'] = datetime.datetime.strptime(record['hora_local'], '%Y-%m-%dT%H:%M:%S.%fZ').time()
        #     record['fecha_y_hora_local'] = datetime.datetime.combine(record['fecha_local'], record['hora_local'])

        sorted_data = sorted(data, key=lambda x: x['fecha_y_hora_local'], reverse=True)[:10]

        records = [
            {
                'referencia': record['referencia'],
                'fecha_local': record['fecha_local'],
                'hora_local': record['hora_local'],
                # 'fecha_y_hora_local': record['fecha_y_hora_local'],
                'magnitud': record['magnitud'],
                'descargas': record['descargas'],
                'id': str(uuid.uuid4())
            }
            for record in sorted_data
        ]

        # Guardar los datos en DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TablaWebScrappingSismos')

        # Eliminar todos los elementos de la tabla antes de agregar los nuevos
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        'id': each['id']
                    }
                )

        # Insertar los nuevos datos
        i = 1
        for row in records:
            row['#'] = i
            table.put_item(Item=row)
            i = i + 1

        # Retornar el resultado como JSON
        return {
            'statusCode': 200,
            'body': records
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
