
# import des librairies
import pandas as pd
import sys
import os
import dotenv
import sqlalchemy
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.types import DateTime

"""
Le fichier load_data.py permet:
- de créer un engine pour se connecter au RDS
- de charger les données transformées dans le RDS
"""



# Définir la base de données
Base = declarative_base()

def load_data_to_rds(df_missions, df_extra, df_hotel, df_logiciel, df_logiciel_extra):
    os.environ.clear()
    # load_dotenv(find_dotenv("/home/gregoirek/Documents/JEDHA/2_Fullstack/x_projet_final/dotenv/.env"))
    load_dotenv(find_dotenv(".env"))
    AWS_RDS_ENDPOINT= os.environ.get("AWS_RDS_ENDPOINT")
    AWS_RDS_REGION=os.environ.get("AWS_RDS_REGION")
    DBNAME = os.environ.get("DBNAME")
    AWS_RDS_USER = os.environ.get("AWS_RDS_USER")
    AWS_RDS_PASSWORD = os.environ.get("AWS_RDS_PASSWORD")
    AWS_RDS_PORT = os.environ.get("AWS_RDS_PORT")

    print("-- Connexion au RDS en cours... --")
    # création du moteur de connexion
    engine = create_engine(f'postgresql+psycopg2://{AWS_RDS_USER}:{AWS_RDS_PASSWORD}@{AWS_RDS_ENDPOINT}:{AWS_RDS_PORT}/{DBNAME}')
    print("Connexion au RDS établie.")
    print("##########################################")


    print(" -- Chargement des données dans le RDS en cours... --")
    # injection des données dans le RDS
    # tant que l'on est en phase de test, on remplace les tables à chaque fois
    try:
        df_extra.to_sql('extra', engine, index=False, if_exists='replace')
        print("Chargement des données dans la table extra terminé.")
    except Exception as e:
        print("Erreur lors du chargement des données dans la table extra : ", e)

    try:
        df_hotel.to_sql('hotel', engine, index=False, if_exists='replace')
        print("Chargement des données dans la table hotel terminé.")
    except Exception as e:
        print("Erreur lors du chargement des données dans la table hotel : ", e)

    try:
        df_missions.to_sql('missions', engine, index=False, if_exists='replace')
        print("Chargement des données dans la table missions terminé.")
    except Exception as e:
        print("Erreur lors du chargement des données dans la table missions : ", e)

    try:
        df_logiciel.to_sql('logiciel', engine, index=False, if_exists='replace')
        print("Chargement des données dans la table logiciel terminé.")
    except Exception as e:
        print("Erreur lors du chargement des données dans la table logiciel : ", e)

    try:
        df_logiciel_extra.to_sql('logiciel_extra', engine, index=False, if_exists='replace')
        print("Chargement des données dans la table logiciel_extra terminé.")
    except Exception as e: 
        print("Erreur lors du chargement des données dans la table logiciel_extra : ", e)

    print("-- Chargement des données dans le RDS terminé. --")
