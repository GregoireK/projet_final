import boto3
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
    connect_s3 = connexion_s3(session)  
    print(connect_s3)

    connect_rds = connexion_rds(session)

    print(connect_rds)


    #transform_missions()


    


if __name__ == "__main__":
    main()
    
  

