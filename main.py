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

from connexion import connexion_s3, connexion_rds, session_boto3
from transform import *
#from load_data import *



def main():
    # fonction de création de la session boto3
    # cette session permettra la connexion avec le S3 et le rds
    session = session_boto3()
    
    # fonction de connexion au S3 bucket qui renvoie trois dataframe pandas df_mission, df_hotel, df_extras
    s3 = connexion_s3(session)   
    df_mission, df_hotel, df_extras = s3[0], s3[1], s3[2]
    # on lance les focntion de transformation auquel on passe en argument le datafram corresondant
    #transform_missions(df_mission)
    #transform_extra(df_extras)


    


if __name__ == "__main__":
    main()
    
  

