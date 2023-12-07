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

colonne_extra = ['nom', 'logiciel', 'Disponibilités', 'dispos', 'vacances', 'statuts','Date de création']
colonne_missions = ['hôtel', 'extra', 'date', 'statuts', 'tarif urgence', 'tarif horaire','facture', 'règlement', 'Texte']


def transform_missions(missions):
    # -- séparation de la colonne date en deux colonnes date_debut et date_fin --
    # suppression de (UTC) dans la colonne date
    df_missions['date'] = df_missions['date'].str.replace(' (UTC)', '')
    df_missions['date'] = df_missions['date'].str.replace(' (UTC+3)', '')
    # je vérifie qu'il n'y a plus de (UTC) dans la colonne date
    print(df_missions['date'].str.contains('UTC+3').value_counts(), df_missions['date'].str.contains('UTC').value_counts())
    print("Suppression de (UTC) et (UTC+3) dans la colonne date terminée.")

    # je sépare la colonne date en cinq colonnes date_debut, heure_début, colonne contenant " → " , date_fin et heure_fin
    df_missions[['date_debut', 'heure_debut', 'vide', 'date_fin', 'heure_fin']] = df_missions['date'].str.split(' ', expand=True)
    
    # si heure_fin est vide, heure_fin = date_fin 
    df_missions['heure_fin'] = np.where(df_missions['heure_fin'].isnull(), 
                                        df_missions['date_fin'], df_missions['heure_fin'])
    # si heure_fin == date_fin, date_fin = date_debut
    df_missions['date_fin'] = np.where(df_missions['heure_fin'] == df_missions['date_fin'], 
                                       df_missions['date_debut'], df_missions['date_fin'])
    # je supprime la colonne vide
    df_missions.drop('vide', axis=1, inplace=True)
    # je fusionne date_debut et heure_debut en une seule colonne date_debut
    df_missions['date_debut'] = df_missions['date_debut'] + " " + df_missions['heure_debut']
    # je fusionne date_fin et heure_fin en une seule colonne date_fin
    df_missions['date_fin'] = df_missions['date_fin'] + " " + df_missions['heure_fin']
    # je supprime les colonnes heure_debut et heure_fin
    df_missions.drop(['heure_debut', 'heure_fin'], axis=1, inplace=True)
    # je transforme les colonnes date_debut et date_fin en datetime
    df_missions['date_debut'] = pd.to_datetime(df_missions['date_debut'], format='%d/%m/%Y %H:%M')
    df_missions['date_fin'] = pd.to_datetime(df_missions['date_fin'], format='%d/%m/%Y %H:%M')

    print("Transformation de la colonne date terminée.")
    # -- fin de la transformation de la colonne date --

    # -- ajout de la colonne durée_mission en heures --
    # je crée une colonne durée_mission qui contient la différence entre date_debut et date_fin
    df_missions['durée_mission'] = df_missions['date_fin'] - df_missions['date_debut']
    # j'obtiens le résultat sous la forme 0 days 12:00:00
    # je conserve uniquement 12:00:00 (position str[7])
    df_missions['durée_mission'] = df_missions['durée_mission'].astype(str).str[7:]
    df_missions['durée_mission'] = pd.to_datetime(df_missions['durée_mission'], format='%H:%M:%S')
    df_missions['durée_mission'] = df_missions['durée_mission'].dt.hour

    print("Ajout de la colonne durée_mission terminée.")
 
    # -- fin de l'ajout de la colonne durée_mission --


    # -- ajout de la colonne TOTAL_HT --
    # je crée une colonne TOTAL_HT qui contient le produit de la colonne durée_mission et de la colonne tarif horaire
    # je rajoute une condition qui vérifie si la colonne tarif urgence est vide ou non
    # j'utilise la fonction np.where qui permet de faire une condition
    df_missions['Total_HT'] = np.where(df_missions['tarif urgence'].isnull(), 
                                       df_missions['durée_mission'] * df_missions['tarif horaire'], 
                                       df_missions['durée_mission'] * df_missions['tarif urgence'])
    
    print("Ajout de la colonne TOTAL_HT terminée.")

    # -- fin de l'ajout de la colonne TOTAL_HT --

    # -- ajout de la colonne TOTAL_TTC --
    # je crée une colonne TOTAL_TTC qui est un produit de df_missions['Total_HT'] et de 1.2
    df_missions['Total_TTC'] = df_missions['Total_HT'] * 1.2

    print("Ajout de la colonne TOTAL_TTC terminée.")

    # -- fin de l'ajout de la colonne TOTAL_TTC --

    # -- ajout de la colonne extra_salaire --
    # les extra sont payé 15€ de l'heure sauf en cas d'urgence ou ils sont payé 20€ de l'heure
    df_missions['extra_salaire'] = np.where(df_missions['tarif urgence'].isnull(), 
                                       df_missions['durée_mission'] * 15, 
                                       df_missions['durée_mission'] * 20)  
    print("Ajout de la colonne extra_salaire terminée.")
    # -- fin de l'ajout de la colonne extra_salaire --

    # -- ajout de la colonne bénéfice --
    # la colonne bénéfice correspond au total ht moins le salaire de l'extra
    df_missions['bénéfice'] = df_missions['Total_HT'] - df_missions['extra_salaire']
    print("Ajout de la colonne bénéfice terminée.")
    # -- fin de l'ajout de la colonne bénéfice --

    print("Toutes les transformations sont terminées.")
    print("######################")



 

   

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
    print("######################")





transform_missions(missions = df_missions)

transform_extra(extra = df)

        









