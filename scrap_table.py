import requests
import boto3
import uuid
import pandas as pd


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
        df = pd.DataFrame(data).rename(columns={'reporte_acelerometrico_pdf': 'descargas'})
        df['fecha_local'] = pd.to_datetime(df['fecha_local'])
        df['hora_local'] = pd.to_datetime(df['hora_local'])
        df['fecha_y_hora_local'] = pd.to_datetime(df['fecha_local']) + pd.to_timedelta(df['hora_local'].dt.strftime('%H:%M:%S'))
        df = df.sort_values(by='fecha_y_hora_local', ascending=False).head(10)

        records = df[['referencia', 'fecha_y_hora_local', 'magnitud', 'descargas']].to_dict(orient='records')
        records = [{**record, 'id': str(uuid.uuid4())} for record in records]

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
