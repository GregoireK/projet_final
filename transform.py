# fichier qui contient l'ensemble des transformations à effectuer sur les données avent chargmeent dans le tbles sur AWS RDS

import pandas as pd
import numpy as np
import os
import sys
import dotenv
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
from datetime import timedelta
import locale

"""
Le fichier Transform.py contient l'ensemble des fonctions qui permettent de:
- crée les foreifn keys
- transformer les données des dataframes missions, hotels et extras
- créer les tables logiciel et join_logiciel_extra
- effectuer un échantillonnage des données
- rescaler les données
- enregistrer les csv
- créer le csv pour le Machine Learning
"""



# definition du language selon l'utilisateur
try:
    locale.setlocale(locale.LC_TIME, 'fr_FR.utf8') 
except:
    pass
try:
    locale.setlocale(locale.LC_TIME, 'fr_FR')
except:
    pass



# récupération des tarifs confidentiels dans le .env
os.environ.clear()

load_dotenv(find_dotenv(".env"))

extra_salaire = float(os.environ.get("extra_salaire"))
extra_salaire_urgence = float(os.environ.get("extra_salaire_urgence"))
tarif_urgence = float(os.environ.get("tarif_urgence"))
sample_size = float(os.environ.get("sample_size"))
coefficient = float(os.environ.get("coefficient"))

# fonction de création des key et des foreign key dans les dataframes extra, hotel et missions
def foreign_key_setup(missions, hotels, extras):
     # on retire les lignes vides pour l'hôtel ou l'extra 
    missions.dropna(subset=['hôtel', 'extra'], inplace=True)
    missions.reset_index(drop=True, inplace=True) # reindexation
    try: 
        hotels.insert(0, 'hotel_id', range(1, 1 + len(hotels)))
        #je rajoute une lettre h à l'index chiffré
        hotels['hotel_id'] = 'h' + hotels['hotel_id'].astype(str)
        extras.insert(0, 'extra_id', range(1, 1 + len(extras)))
        extras['extra_id'] = 'e' + extras['extra_id'].astype(str)
        missions.insert(0, 'mission_id', range(1, 1 + len(missions)))
        missions['mission_id'] = 'm' + missions['mission_id'].astype(str)
    except:
        pass
    # on remplace le nom de l'hote par son id dans la colonne hôtel de la dataframe missions
    liste_hotel_id = []
    liste_extra_id = []
    for i in missions['hôtel']:
        try:
            liste_hotel_id.append(hotels['hotel_id'].where(hotels['nom'] == i).dropna().values[0])
        except:
            pass
    # on remplace le nom de l'extra par son id dans la colonne extra de la dataframe missions
    for i in missions['extra']:
        try:
            liste_extra_id.append(extras['extra_id'].where(extras['nom'] == i).dropna().values[0])
        except:
            pass
    missions['hôtel'] = liste_hotel_id
    missions['extra'] = liste_extra_id
    print("Mise en place des foreigns keys terminée.")
    return missions, hotels, extras

# fonction de transformation et d'ajout de colonnes dans la dataframe missions
def transform_missions(missions):
    # -- Transformation de la colonne date en date_debut et date_fin --
    print("######################")
    print('-- Phase de transformation pour les missions --')
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

    missions.drop('Texte', axis=1) # suppression de la colonne Texte
    print("Ajout de la colonne heures_supp terminée.")

    # -- fin de l'ajout de la colonne heures_supp --

    # -- ajout de la colonne TOTAL_HT --
    # 0€ si annulation, duree_mission * tarif horaire si non urgent, duree_mission * tarif urgence si urgent
    missions['Total_HT'] = missions.apply(lambda row: 0 if pd.notna(row['annulation'])
                                    else row['duree_mission'] * row['tarif horaire'] if not row['urgent']
                                    else row['duree_mission'] * row['tarif urgence'], axis=1)

    # ajout des heures supp en tarif urgence 
    missions['Total_HT'] += missions['heures_supp'].apply(lambda x: x * tarif_urgence if pd.notna(x) else 0)    

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

    # -- ajout de la colonne benefice --
    # la colonne benefice correspond au total ht moins le salaire de l'extra
    missions['benefice'] = missions['Total_HT'] - missions['extra_salaire']
    print("Ajout de la colonne benefice terminée.")
    # -- fin de l'ajout de la colonne benefice --

     # on renomme les colonnes pour enlever les caractères unicodes qui posent problème pour la connexion PostgreSQL - Looker
    nvlles_cols = {'hôtel':'hotel_id', 'extra': 'extra_id', 'tarif urgence':'tarif_urgence', 
                   'tarif horaire': 'tarif_horaire', 'règlement':'reglement'}
    missions.rename(columns=nvlles_cols, inplace=True)

    print("-- Toutes les transformations sont terminées. --")
    print("######################")

    return missions

# fonction de transformation et d'ajout de colonnes dans la dataframe extra
def transform_extra(extras):
    print("######################")
    print('-- Phase de transformation pour les extras --')

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
    extras.drop(['vacances'], axis=1, inplace=True)
    print("Transformation de la colonne vacances terminée.")
   

    # -- Transformation de la colonne Disponibilités --
    # je fusionne les colonnes disponibilités et dispos

    extras['disponibilités'] = extras['Disponibilités'] + " , " + extras['dispos']
    # je supprime les colonnes dispos et Disponibilités
    extras.drop(['dispos', 'Disponibilités'], axis=1, inplace=True)

    extras['date_de_creation'] = extras['Date de création']
    extras['date_de_creation'] = extras['date_de_creation'].apply(lambda x: datetime.strptime(x, "%d %B %Y %H:%M"))
    extras.drop(['Date de création'], axis=1, inplace=True)
    print("Transformation de la colonne Disponibilités terminée.")
    # -- fin de la transformation de la colonne Disponibilités --

    print("-- Toutes les transformations sont terminées.--")
    print("######################")

    return extras

# fonction de transformation et d'ajout de colonnes dans la dataframe hotel
def transform_hotel(hotels):
    print("######################")
    print('-- Phase de transformation pour les hotels --')
    # -- Transformation de la colonne 'Date de création' en datetime --
    hotels['Date de création'] = hotels['Date de création'].apply(lambda x: datetime.strptime(x, "%d %B %Y %H:%M"))
    print("Transformation de la colonne 'Date de création' terminée.")

    # -- On enleve les espaces au debut et à la fin de toutes les colonnes texte --
    hotels = hotels.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # on renomme les colonnes pour enlever les caractères unicodes qui posent problème pour la connexion PostgreSQL - Looker
    nvlles_cols = {'tarif horaire ':'tarif_horaire', 'MAJ procédures': 'MAJ_procedure', 
                   'Dernier contact Arnaud': 'last_contact', 'Date de création':'date_de_creation'}
    hotels.rename(columns=nvlles_cols, inplace=True)

    print("-- Toutes les transformations sont terminées. --")
    print("######################")
    return hotels

# fonction de création de la table logiciel
def create_table_logiciel(extras):
    # je crée une liste qui rassemble tous les logiciels dans le dataframe extra. je split et je supprimme les doublons
    logiciel = extras['logiciel'].tolist()
    # je boucle sur la liste pour appliquer la méthode split et je passe le résultat dans une liste
    # attntion les valaeurs nan(type float) provoque une erreur
    logiciels_liste = []
    for i in logiciel:
        if type(i) is float:
            pass
        else:
            logiciels_liste.append(i.split(','))
    # je supprime les espaces en début de chaine de caractère
    logiciels_liste = [[j.strip() for j in i] for i in logiciels_liste]
    # je supprime les doublons dans la liste avec la fonction set()
    logiciels_liste = set([item for sublist in logiciels_liste for item in sublist])
    # j'obtiens une iste de 18 noms de logiciels.
    # je crée un dataframe avec les noms de logiciels
    logiciel_df = pd.DataFrame(logiciels_liste, columns=['logiciel_name'])
    # je rajoute une colonne logiciel_id
    logiciel_df.insert(0, 'logiciel_id', range(1, 1 + len(logiciel_df)))
    logiciel_df['logiciel_id'] = 'l' + logiciel_df['logiciel_id'].astype(str)
    print("Création de la table logiciel terminée.")
    return logiciel_df

def join_table_logiciel_extra(extras, logiciel_df):
    # -- Etape 1 --
    # je selectione les colonnes de extra sur lequelle je vais travailler
    # je veux extra_id et les logiciel sous la forme d'une liste
    extract_extra = extras[['extra_id', 'logiciel']]
    # Fonction pour transformer la chaîne de caractères en une liste de logiciels
    def split_logiciels(logiciels_string):
        if type(logiciels_string) is float:
            pass
        else:
            return logiciels_string.split(', ')
    # je lance a fonction split avec .apply() sur la colonne logiciel
    try:
        extract_extra['logiciel'] = extract_extra['logiciel'].apply(split_logiciels)
    except:
        pass
    # -- Etape 2 -- 
    # je crée un dataframe qui regroupe les associations extra_id - logiciel_id
    # on utilise la fonction .apply() qui va appliquer sur chaque ligne la fonction process_row
    # la fonction process_row que va récuperer les informations et les renvoyer sous forme de DataFrame
    # process_row() prend la ligne en argument = on obtiens l'extra_id et la liste des logiciels
    # sur cette ligne à chaque fois que le nom d'un logiciel est présent on séléctionne l'id du logiciel et l'id de l'extra
    # (si la valeur est nan, on passe)
    # les données sont ajouté à une liste (fonction .append) qui est ensuite transformée en DataFrame
    # on utilise la fonction pd.concat() pour concaténer les DataFrame
    # la dataframe finale est renvoyéé et représente un jointure entre l'extra_id et le logicel_id

    # Fonction personnalisée à appliquer avec apply en argument chqaue ligne
    def process_row(row):
        extra_logiciels = row['logiciel']
        extra_id = row['extra_id']
        # liste vide dans laquelle on va ajouter les associations extra_id - logiciel_id - logiciel_name
        rows_to_append = []
        # si la valeur est nan, on retourne un DataFrame vide
        if type(extra_logiciels) is not list:
            pass
        # on crée une liste vide dans laquelle on ajoute chaque association
        # extra_id - logiciel_id - logiciel_name
        else:
            # extra_logiciels est une liste de logiciels on boucle dessus
            for logiciel in extra_logiciels:
                if logiciel in logiciel_df['logiciel_name'].tolist():
                    logiciel_id = logiciel_df.loc[logiciel_df['logiciel_name'] == logiciel, 'logiciel_id'].values
                    rows_to_append.append({'extra_id': extra_id, 'logiciel_id': logiciel_id[0]})
        # on renvoie la liste sous forme de dataframe
        return pd.DataFrame(rows_to_append)

    join_table = pd.concat(extract_extra.apply(process_row, axis=1).tolist(), ignore_index=True)
    print("Création de la table join_logiciel_extra terminée.")
    return join_table

# fonction d'échantilonaage des données
def echantillonnage(missions, hotels, extras):
    print("######################")
    print('-- Phase d\'échantillonnage --')
    # on prend un echantillon des hotels et des extras pour masquer le nombre de clients réels
    # sample_size est une valeur cachée dans le .env
    hotels = hotels.sample(frac=sample_size, random_state=1)
    extras = extras.sample(frac=sample_size, random_state=1)

    # on récupère la liste d'hotels et d'extras de l'échantillon
    valeurs_hotels = hotels['hotel_id'].unique()
    valeurs_extras = extras['extra_id'].unique()

    # on créé des conditions pour garder les missions ayant un extra et un hôtel inclus dans les échantillons pris ci-dessus
    cond_hotels = missions['hotel_id'].isin(valeurs_hotels)
    cond_extras = missions['extra_id'].isin(valeurs_extras)

    # on filtre les missions à l'aide des conditions
    missions = missions[cond_hotels & cond_extras]
    print("######################")
    print("-- Echantillonnage terminé. --")
    # on retourne les 3 dataframes échantillonnées
    return missions, hotels, extras

# fonction qui permet un rescale des données financières dans les dataframes missions et hotels
def rescale(missions, hotels):
    print("######################")
    print('-- Phase de rescaling --')
    # colonnes a rescaler dans le dataframe missions
    colonnes_missions = ['tarif_urgence', 'tarif_horaire', 'Total_HT', 'Total_TTC', 'extra_salaire', 'benefice']
    missions.loc[:, colonnes_missions] *= coefficient

    # colonne a rescaler dans le dataframe hotels
    colonne_hotels = ['tarif_horaire']
    hotels.loc[:, colonne_hotels] *= coefficient

    # on retourne les 2 dataframes rescalés
    print("-- Rescaling terminé. --")
    return missions, hotels
  
# fonction d'enregistrement des csv
def print_csv(missions_transform, extra_transform, hotel_transform, logiciel_df, join_table, ml_table):
    print("######################")
    print("Phase d'enregistrement des CSV")
    # on définit le chemin du fihcier python qui est lancé
    __file__ = sys.argv[0]

    # dans la variable dirpath = on crée un chemin absolue à partir du chemin relatif du fichier python
    # dans la variable dirname = on crée le nom du dossier que l'on veut créer
    filepath, dirname = os.path.dirname(os.path.abspath(__file__)), "CSV"
    # on crée le chemin absolu du dossier que l'on veut créer avec le dossier parent et le nome du dossier
    dirpath = os.path.join(filepath, dirname)

    # si le dossier CSV existe pas alors on le crée
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)
    else:
        pass

    # on enregiste les CSV dans le dossier CSV
    missions_transform.to_csv("{}/missions_transform_sampling.csv".format(dirpath))
    extra_transform.to_csv("{}/extra_transform_sampling.csv".format(dirpath))
    hotel_transform.to_csv("{}/hotel_transform_sampling.csv".format(dirpath))
    logiciel_df.to_csv("{}/logiciel.csv".format(dirpath))
    join_table.to_csv("{}/join_logiciel_extra.csv".format(dirpath))
    ml_table.to_csv("{}/ml_missions_par_semaine.csv".format(dirpath), index=False)

    print("CSV enregistrés.")
    print("######################") 

# fonction d'enregistrement du csv pour le ML avec les données aggrégées par semaine
def ml_transform_semaine(missions_transform):
    print("######################")
    print("Création des données pour la prédiction.")
    # on intialise le dataframe qui va stocker les données aggrégées
    df = pd.DataFrame()

    # on détermine la semaine ISO à partir de la date
    df['iso_week'] = missions_transform['date_debut'].dt.strftime('%G-W%V')

    # on aggrége le nombre de missions par semaine ISO
    df = df.groupby('iso_week').size().reset_index()
    df.columns = ['iso_week', 'total_missions']

    # on supprime la première semaine du dataset au cas où elle n'est pas complète
    premiere_semaine = min(df['iso_week'])
    df = df[df['iso_week'] != premiere_semaine]

    # on retire toutes les semaines supérieures ou égales à la semaine d'extraction du fichier (ici 2023-W48)
    df = df[df['iso_week'] <= '2023-W48']

    # on determine la datetime correspondant au debut de chaque semaine car le modèle de prédiction a besoin d'un datetime
    df['start_of_week'] = df['iso_week'].apply(lambda x: datetime.fromisoformat(x + '-1'))

    # on retire la colonne 'iso_week'
    df.drop(columns=['iso_week'], inplace=True)
    df.columns = ['total_missions', 'start_of_week']
    # on rearrange l'ordre des colonnes
    df = df[['start_of_week', 'total_missions']]

    # on calcule les datetime correspondant aux 4 prochaines semaines
    last_start_of_week = df['start_of_week'].iloc[-1]
    future_start_of_weeks = pd.date_range(start=last_start_of_week + timedelta(days=7), periods=4, freq='7D')

    # on créé un dataframe avec les futures dates et le total_missions vide
    future_dates_df = pd.DataFrame({'start_of_week': future_start_of_weeks,
                                'total_missions': [None] * 4})
    
    # dataframe final pour la prédiction
    df = pd.concat([df, future_dates_df], ignore_index=True)
        # si le dossier CSV existe pas alors on le crée
    print("Donnée ML pour la prédiction crée.")
    return df


        









