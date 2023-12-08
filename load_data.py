
# fichier de chargement des donnée dans le RDS 
import boto3
import psycopg2
import pandas as pd
import sys
import os
import dotenv
import sqlalchemy
from io import BytesIO
from io import StringIO
import botocore
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.types import DateTime





# Définir la base de données
Base = declarative_base()

def load_data(df_missions, df_extra, df_hotel):
    os.environ.clear()
    load_dotenv(find_dotenv("/home/gregoirek/Documents/JEDHA/2_Fullstack/x_projet_final/dotenv/.env"))
    AWS_RDS_ENDPOINT= os.environ.get("AWS_RDS_ENDPOINT")
    AWS_RDS_REGION=os.environ.get("AWS_RDS_REGION")
    DBNAME = os.environ.get("DBNAME")
    AWS_RDS_USER = os.environ.get("AWS_RDS_USER")
    AWS_RDS_PASSWORD = os.environ.get("AWS_RDS_PASSWORD")
    AWS_RDS_PORT = os.environ.get("AWS_RDS_PORT")

    # Créer le moteur de base de données
    engine = create_engine(f'postgresql+psycopg2://{AWS_RDS_USER}:{AWS_RDS_PASSWORD}@{AWS_RDS_ENDPOINT}:{AWS_RDS_PORT}/{DBNAME}')

    # Remplacez ces valeurs par vos DataFrames
    df_missions.to_sql('missions', engine, index=False, if_exists='append')
    df_extra.to_sql('extra', engine, index=False, if_exists='append')
    df_hotel.to_sql('hotel', engine, index=False, if_exists='append')

    print("Chargement des données dans le RDS terminé.")