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

#from anonymisation import *
from connexion import *
from transform import *
from load_data import *


"""
Le fichier main.py appelle les différents modules du projet.
Il permet de récupérer les données anonymisées dans un bucket S3.
Il permet de créer les clés d'identification.
Il permet de transformer les données.
Il permet de sampler les données.
Il permet de rescaler les données.
Il permet également de créer les tables pour la prédiction et de les sauvegarder en CSV dans un dossier.
Il permet enfin de charger les tables dans un RDS.

"""


def main():

    # -- anonimisation des données --
    # -- sauvegarde des données anonymisées dans un bucket S3 -- 

    # -- connexion au bucket S3 --
    session = session_boto3()

    # -- récupération des données anonymisées -- 
    s3 = connexion_s3(session)   
    mission, hotel, extras = s3[0], s3[1], s3[2]

    # -- création des clés d'identification --

    fk = foreign_key_setup(mission, hotel, extras)
    fk_mission, fk_hotel, fk_extra = fk[0], fk[1], fk[2]

    # -- transformation des données --

    transform_m = transform_missions(fk_mission)
    transform_e = transform_extra(fk_extra)
    transform_h = transform_hotel(fk_hotel)

    logiciel = create_table_logiciel(transform_e)

    # -- sampling des données et rescaling --

    sampling = echantillonnage(transform_m, transform_h, transform_e)

    missions_transform_s= sampling[0]
    hotel_transform_s = sampling[1]
    extras_transform_s = sampling[2]

     # -- rescaling des données financières dans les tables missions et hotels -- 

    rescaling = rescale(missions_transform_s, hotel_transform_s)
    missions_transform_final = rescaling[0]
    hotel_transform_final = rescaling[1]
    extras_transform_final = extras_transform_s

    join_table = join_table_logiciel_extra(extras = extras_transform_final, logiciel_df = logiciel )

    # -- je créé le fichier pour la prédiction --
    ml_transform_semaine(missions_transform_final)

    # -- print en csv (optionnel) --
    print_csv(missions_transform=missions_transform_final,
                extra_transform=extras_transform_final,
                hotel_transform=hotel_transform_final,
                logiciel_df=logiciel,
                join_table=join_table,
                ml_table=ml_transform_semaine(missions_transform_final))

    # -- connexion au RDS et transfère des données --
    
    load_data_to_rds(df_missions=missions_transform_final, 
                     df_extra=extras_transform_final, 
                     df_hotel=hotel_transform_final, 
                     df_logiciel=logiciel, 
                     df_logiciel_extra=join_table)


if __name__ == "__main__":
    main()
    
  

