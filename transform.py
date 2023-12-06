# fichier qui contient l'ensemble des transformations à effectuer sur les données avent chargmeent dans le tbles sur AWS RDS

import pandas as pd
import numpy as np
import datetime
from dateutil import parser
import math
import boto3
import io
import os
import dotenv
from dotenv import load_dotenv


#  -- transformation de la table missions --
# ajout de la colonne Total_HT
# ajout de la colonne Total_TTC
# ajout de la colonne Bénéfice
# ajout de la colonne salaire_extra
# transformation de la colonne date en deux colonnee date_debut et date_fin
# ajout d'une colonne durée_mission en heures
# ajout d'une colonne heure_sup

df = pd.read_csv('/home/gregoirek/Documents/JEDHA/2_Fullstack/x_projet_final/extras_anonymes.csv')

df_missions = pd.read_csv('/home/gregoirek/Documents/JEDHA/2_Fullstack/x_projet_final/missions_anonymes.csv')

def transform_missions(missions):
    print(df_missions.columns)

colonne_extra = ['nom', 'logiciel', 'Disponibilités', 'dispos', 'vacances', 'statuts','Date de création']
colonne_missions = ['hôtel', 'extra', 'date', 'statuts', 'tarif urgence', 'tarif horaire','facture', 'règlement', 'Texte']

def transform_extra(extra):
    liste_mois = ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet',
              'août', 'septembre', 'octobre', 'novembre', 'décembre']
    # -- Transformation de la colonne vacances --
    # je parcours la colonne vacances et je remplace les mois par leur numéro
    for i, v in enumerate(liste_mois):
        # i : index de 0 à 11
        # v mois de janvier à décembre
        # on remplace les mois par leur numéro on précise i+1 car l'index commence à 0
        df['vacances'] = df['vacances'].str.replace(v, str(i+1))

    # je sépare la colonne vacances en deux colonnes vacances_debut et vacances_fin
    df['vacance_debut'] = df['vacances'].str.split(' → ').str[0]
    df['vacance_fin'] = df['vacances'].str.split(' → ').str[1]

    # les dates sont sous la forme 01 01 2020 00:00 je les transforme en datetime
    df['vacance_debut'] = pd.to_datetime(df['vacance_debut'], format='%d %m %Y %H:%M') 
    df['vacance_fin'] = pd.to_datetime(df['vacance_fin'], format='%d %m %Y %H:%M')
    print("Transformation de la colonne vacances terminée.")
    # -- fin de la transformation de la colonne vacances --

    # -- Transformation de la colonne Disponibilités --
    # je fusionne les colonnes disponibilités et dispos

    df['Dispobilités'] = df['Disponibilités'] + " , " + df['dispos']
    # je supprime les colonnes dispos et Disponibilités
    df.drop(['dispos', 'Disponibilités'], axis=1, inplace=True)
    print("Transformation de la colonne Disponibilités terminée.")
    # -- fin de la transformation de la colonne Disponibilités --

    print("Toutes les transformations sont terminées.")





transform_missions(df_missions)

#transform_extra(df)

        









