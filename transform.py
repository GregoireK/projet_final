# fichier qui contient l'ensemble des transformations à effectuer sur les données avent chargmeent dans le tbles sur AWS RDS

import pandas as pd
import numpy as np
import os
import dotenv
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
import locale
locale.setlocale(locale.LC_TIME, 'fr_FR')

# récupération des tarifs confidentiels dans le .env
os.environ.clear()
load_dotenv(find_dotenv(".env"))
extra_salaire = float(os.environ.get("extra_salaire"))
extra_salaire_urgence = float(os.environ.get("extra_salaire_urgence"))

# import des fichiers anonymisés
extras = pd.read_csv('fichiers-anonymes/extras_anonymes.csv')
missions = pd.read_csv('fichiers-anonymes/missions_anonymes.csv')
hotels = pd.read_csv('fichiers-anonymes/hotel_anonymes.csv')

#  -- colonnes finales de la table missions --
# ['hôtel', 'extra', 'tarif urgence', 'tarif horaire', 'facture', 'règlement', 'date_debut', 'date_fin', 'duree_mission', 'urgent',
#      'annulation', 'heures_supp', 'Total_HT', 'Total_TTC', 'extra_salaire', 'bénéfice']

def transform_missions(missions):
    # -- Transformation de la colonne date en date_debut et date_fin --

    missions.dropna(subset=['hôtel', 'extra'], inplace=True) # on retire les lignes vides pour l'hôtel ou l'extra 
    missions.reset_index(drop=True, inplace=True) # reindexation

    # on retire (UTC) et (UTC+3) dans la colonne date
    missions['date'] = missions['date'].str.replace('(UTC)', '') 
    missions['date'] = missions['date'].str.replace('(UTC+3)', '')

    # On separe la colonne date en date_debut et date_fin
    missions['date_debut'] = missions['date'].str.split(' → ').str[0]
    missions['date_fin'] = missions['date'].str.split(' → ').str[1]

    # on retire les espaces qui peuvent poser problème au début et à la fin des colonnes
    missions['date_debut'] = missions['date_debut'].str.strip()
    missions['date_fin'] = missions['date_fin'].str.strip()

    nb_lignes = missions.shape[0]

    # la colonne date_fin ne contient pas toujours de date, parfois il y a uniquement l'heure de fin indiquée
    for i in range(nb_lignes):
        if '/' in missions.loc[i, 'date_fin']: # si la date_fin contient déjà une date, on la conserve
            missions.loc[i, 'date_fin'] = missions.loc[i, 'date_fin'] 
        else: # sinon, on ajoute la date_debut à l'heure dans la colonne date_fin
            missions.loc[i, 'date_fin'] = missions.loc[i,'date_debut'].split(' ')[0] + ' ' + missions.loc[i, 'date_fin']

    # conversion des colonnes date_debut et date_fin en datetime 
    missions['date_debut'] = pd.to_datetime(missions['date_debut'], format='%d/%m/%Y %H:%M')
    missions['date_fin'] = pd.to_datetime(missions['date_fin'], format='%d/%m/%Y %H:%M')

    missions.drop('date', axis=1, inplace=True) # suppression de la colonne date

    print("Transformation de la colonne date terminée.")

    # -- fin de la transformation de la colonne date --

    # -- Ajout d'une colonne duree_mission en heures -- 
    missions['duree_mission'] = (missions['date_fin'] - missions['date_debut']).dt.total_seconds()/3600
    print("Ajout de la colonne duree_mission terminée.")
 
    # -- fin de l'ajout de la colonne durée_mission --

    # -- Transformation de la colonne 'statuts' en colonnes 'urgent' et 'annulation'
    missions['urgent'] = missions['statuts'].apply(lambda x: 'urgente' in x if pd.notna(x) else False )

    missions['annulation'] = missions['statuts'].apply(lambda x: 'extra' if x == 'annulé extra' 
                                                    else 'hôtel' if x == 'annulé hôtel'
                                                    else np.nan)
    
    missions.drop('statuts', axis=1, inplace=True) # suppression de la colonne statuts

    # -- Ajout d'une colonne heures_supp et suppression de la colonne Texte --
    replace_supp = {'30 min supp': 0.5, '1h30 supp': 1.5, '3h supp': 3, # dictionnaire qui remplace le texte par la duree en heures 
                '2h supp': 2, '1h supp': 1, '2h30 heures supp': 2.5, 
                '30 minutes heures supp': 0.5}
    
    missions['heures_supp'] = missions['Texte'].replace(replace_supp)

    missions.drop('Texte', axis=1, inplace=True) # suppression de la colonne Texte
    print("Ajout de la colonne heures_supp terminée.")

    # -- fin de l'ajout de la colonne heures_supp --

    # -- ajout de la colonne TOTAL_HT --
    # 0€ si annulation, duree_mission * tarif horaire si non urgent, duree_mission * tarif urgence si urgent
    missions['Total_HT'] = missions.apply(lambda row: 0 if pd.notna(row['annulation'])
                                    else row['duree_mission'] * row['tarif horaire'] if not row['urgent']
                                    else row['duree_mission'] * row['tarif urgence'], axis=1)

    # ajout des heures supp en tarif urgence 
    missions['Total_HT'] += missions['heures_supp'].apply(lambda x: x * 25 if pd.notna(x) else 0)    

    print("Ajout de la colonne TOTAL_HT terminée.")
                            
    # -- fin de l'ajout de la colonne TOTAL_HT --

    # -- ajout de la colonne TOTAL_TTC --
    # je crée une colonne TOTAL_TTC qui est un produit de extras_missions['Total_HT'] et de 1.2
    missions['Total_TTC'] = missions['Total_HT'] * 1.2

    print("Ajout de la colonne TOTAL_TTC terminée.")

    # -- fin de l'ajout de la colonne TOTAL_TTC --

    # -- ajout de la colonne extra_salaire --
    # 0€ si annulation, duree_mission * extra_salaire si non urgent, duree_mission * extra_salaire_urgence si urgent
    missions['extra_salaire'] = missions.apply(lambda row: 0 if pd.notna(row['annulation'])
                                    else row['duree_mission'] * extra_salaire if not row['urgent']
                                    else row['duree_mission'] * extra_salaire_urgence, axis=1)

    # ajout des heures supp en salaire urgence 
    missions['extra_salaire'] += missions['heures_supp'].apply(lambda x: x * extra_salaire_urgence if pd.notna(x) else 0)   
    print("Ajout de la colonne extra_salaire terminée.")
    # -- fin de l'ajout de la colonne extra_salaire --

    # -- ajout de la colonne bénéfice --
    # la colonne bénéfice correspond au total ht moins le salaire de l'extra
    missions['bénéfice'] = missions['Total_HT'] - missions['extra_salaire']
    print("Ajout de la colonne bénéfice terminée.")
    # -- fin de l'ajout de la colonne bénéfice --

    print("Toutes les transformations sont terminées.")
    print("######################")

 

   

def transform_extra(extras):
    liste_mois = ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet',
              'août', 'septembre', 'octobre', 'novembre', 'décembre']
    # -- Transformation de la colonne vacances --
    # je parcours la colonne vacances et je remplace les mois par leur numéro
    for i, v in enumerate(liste_mois):
        # i : index de 0 à 11
        # v mois de janvier à décembre
        # on remplace les mois par leur numéro on précise i+1 car l'index commence à 0
        extras['vacances'] = extras['vacances'].str.replace(v, str(i+1))

    # je sépare la colonne vacances en deux colonnes vacances_debut et vacances_fin
    extras['vacance_debut'] = extras['vacances'].str.split(' → ').str[0]
    extras['vacance_fin'] = extras['vacances'].str.split(' → ').str[1]

    # les dates sont sous la forme 01 01 2020 00:00 je les transforme en datetime
    extras['vacance_debut'] = pd.to_datetime(extras['vacance_debut'], format='%d %m %Y %H:%M') 
    extras['vacance_fin'] = pd.to_datetime(extras['vacance_fin'], format='%d %m %Y %H:%M')
    extras.drop('vacances', axis=1, inplace=True) # suppresion de la colonne 'vacances'

    print("Transformation de la colonne vacances terminée.")
    # -- fin de la transformation de la colonne vacances --

    # -- Transformation de la colonne Disponibilités --
    # je fusionne les colonnes disponibilités et dispos

    extras['Dispobilités'] = extras['Disponibilités'] + " , " + extras['dispos']
    # je supprime les colonnes dispos et Disponibilités
    extras.drop(['dispos', 'Disponibilités'], axis=1, inplace=True)
    print("Transformation de la colonne Disponibilités terminée.")
    # -- fin de la transformation de la colonne Disponibilités --

    # -- Transformation de la colonne 'Date de création' en datetime --
    extras['Date de création'] = extras['Date de création'].apply(lambda x: datetime.strptime(x, "%d %B %Y %H:%M"))
    print("Transformation de la colonne 'Date de création' terminée.")

    print("Toutes les transformations sont terminées.")
    print("######################")


def transform_hotel(hotels):
    # -- Transformation de la colonne 'Date de création' en datetime --
    hotels['Date de création'] = hotels['Date de création'].apply(lambda x: datetime.strptime(x, "%d %B %Y %H:%M"))
    print("Transformation de la colonne 'Date de création' terminée.")

    print("Toutes les transformations sont terminées.")
    print("######################")

# application des transformations
transform_missions(missions)
transform_extra(extras)
transform_hotel(hotels)

# export des fichiers transformés en csv
missions.to_csv('missions_viz.csv', index=False)
extras.to_csv('extras_viz.csv', index=False)
hotels.to_csv('hotels_viz.csv', index=False)

# nom des colonnes finales
print(missions.columns)
print(extras.columns)
print(hotels.columns)

        









