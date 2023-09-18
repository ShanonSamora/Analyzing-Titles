import os
import boto3
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

BUCKET_NAME = os.getenv('BUCKET_NAME')
DOWNLOADS_PATH = './downloads/'

# Las variables de entorno necesarias para las credenciales de acceso deben estar configuradas en un .env antes de ejecutar el código.
def download_from_s3(bucket_name, file_name, download_path):
    s3_client = boto3.client(
        's3',
        aws_access_key_id = os.getenv('ACCESS_KEY'),
        aws_secret_access_key = os.getenv('SECRET_KEY')
    )
    
    s3_client.download_file(bucket_name, file_name, download_path + file_name)
    print(f'{file_name} downloaded.')

# Primer archivo: Disney plus titles
first_file_name = 'disney_plus_titles.csv'

download_from_s3(BUCKET_NAME, first_file_name, DOWNLOADS_PATH)

# Segundo archivo: Netflix titles
second_file_name = 'netflix_titles.csv'

download_from_s3(BUCKET_NAME, second_file_name, DOWNLOADS_PATH)