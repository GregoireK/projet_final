import boto3
import pandas as pd
import io
import os
import dotenv
from dotenv import load_dotenv
from connexion import connexion_s3
from transform import *
from load_data import *



def main():
    connect = connexion_s3()  
    print(connect)

    #transform_missions()


    


if __name__ == "__main__":
    main()
    
  

