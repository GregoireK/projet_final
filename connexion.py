import boto3
import pandas as pd
import sys
import os
from io import BytesIO
from io import StringIO
import botocore
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv, find_dotenv

"""
Le fichier connexion.py permet:
- de crée une session avec boto3
- de se connecter au bucket S3
- de récupérer les csv

"""

def session_boto3():
    os.environ.clear()
    # load_dotenv(find_dotenv("/home/gregoirek/Documents/JEDHA/2_Fullstack/x_projet_final/dotenv/.env"))
    load_dotenv(find_dotenv(".env"))

    aws_access_key_id=os.environ.get("AWS_S3_ACCES_KEY_ID")
    aws_secret_access_key=os.environ.get("AWS_S3_SECRET_ACCESS_KEY")
    region_name = 'eu-west-3'

    # je créer une session avec boto3 dans laquel je passe mes key (du fihcier .env)
    # la .session() de boto" meconnecte à AWS
    session = boto3.Session(aws_access_key_id=aws_access_key_id, 
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=region_name)
    
    return session

def connexion_s3(session):

    
    # la variable session contient la session de connexion 
    # avec .resosurce() je précise que je veux acceder à s3
    s3 = session.client("s3")

    bucket = "bucketprojetfinal"

    key_missions = "missions_anonymes.csv"
    key_hotel = "hotel_anonymes.csv"
    key_extras = "extras_anonymes.csv"

    try:
       # on tuilise la méthode get_object de boto3 pour récupérer les csv
       # on précise le bucket et la key du csv
       # on utilise la méthode read() pour lire le csv
        missions = s3.get_object(Bucket=bucket, Key=key_missions)
        missions_csv_content = missions['Body'].read().decode('utf-8')

        hotels = s3.get_object(Bucket=bucket, Key=key_hotel)
        hotels_csv_content = hotels['Body'].read().decode('utf-8')

        extras = s3.get_object(Bucket=bucket, Key=key_extras)
        extra_csv_content = extras['Body'].read().decode('utf-8')

        # on utilise StringIO pour lire le csv avec pandas
        df_missions = pd.read_csv(StringIO(missions_csv_content))
        df_hotel = pd.read_csv(StringIO(hotels_csv_content))
        df_extras = pd.read_csv(StringIO(extra_csv_content))

        return df_missions, df_hotel, df_extras
    
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
        return None





