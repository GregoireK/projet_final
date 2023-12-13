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
from connexion import connexion_s3, session_boto3
from transform import *
from load_data import *



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

    # -- sampling des données --

    sampling = echantillonnage(transform_m, transform_h, transform_e)

    missions_transform_final = sampling[0]
    hotel_transform_final = sampling[1]
    extras_transform_final = sampling[2]

    join_table = join_table_logiciel_extra(extras = extras_transform_final, logiciel_df = logiciel )

    # -- connexion au RDS et transfère des données --


    # -- print en csv (optionnel) --
    print_csv(missions_transform=missions_transform_final,
                extra_transform=extras_transform_final,
                hotel_transform=hotel_transform_final,
                logiciel_df=logiciel,
                join_table=join_table)
    
    load_data_to_rds(df_missions=missions_transform_final, 
                     df_extra=extras_transform_final, 
                     df_hotel=hotel_transform_final, 
                     df_logiciel=logiciel, 
                     df_logiciel_extra=join_table)

   


if __name__ == "__main__":
    main()
    
  

