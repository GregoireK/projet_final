import boto3
import botocore
import pandas as pd
import io
import os
import dotenv
from dotenv import load_dotenv
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
    
  

