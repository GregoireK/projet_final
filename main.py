
"""
Grégoire et Sarah

Le fichier main.py appelle les différents modules du projet.
Il permet de récupérer les données anonymisées dans un bucket S3.
Il permet de créer les clés d'identification.
Il permet de transformer les données.
Il permet de sampler les données.
Il permet de rescaler les données.
Il permet également de créer les tables pour la prédiction et de les sauvegarder en CSV dans un dossier.
Il permet enfin de charger les tables dans un RDS.

"""

# import des fichiers associés
#from anonymisation import *
from connexion import *
from transform import *
from load_data import *

"""
Le fichier main.py appelle les différents modules du projet.
"""
def main():

    # -- anonimisation des données --
    # -- sauvegarde des données anonymisées dans un bucket S3 -- 

    # -- connexion au bucket S3 --
    """
    La fonction session_boto3() permet de créer une session AWS avec boto3.
    La variable session contient la session de connexion.
    cette variable est retourné pour créer la connexion au bucket S3.
    """
    session = session_boto3()

    """
    La fonction connexion_s3(session) permet de se connecter au bucket S3.
    Elle permet également de récupérer les csv.
    Elle retourne les csv.
    """
    # -- récupération des données anonymisées -- 
    s3 = connexion_s3(session)   
    mission, hotel, extras = s3[0], s3[1], s3[2]

    # -- création des clés d'identification --
    """
    La fonction foreign_key_setup(mission, hotel, extras) permet de créer les clés d'identification.
    Elle retourne les dataframes avec les clés d'identification.
    """
    fk = foreign_key_setup(mission, hotel, extras)
    fk_mission, fk_hotel, fk_extra = fk[0], fk[1], fk[2]

    """
    Les fonctions transform_missions(fk_mission), 
    transform_extra(fk_extra) 
    et transform_hotel(fk_hotel) 
    permettent d'effectuer l'ensemble des transformations nécéssaires sur les dataframes.
    Elles retournent les dataframes transformés.
    """
    # -- transformation des données --
    transform_m = transform_missions(fk_mission)
    transform_e = transform_extra(fk_extra)
    transform_h = transform_hotel(fk_hotel)
    """
    La fonction create_table_logiciel(transform_e) 
    permet de créer la table logiciel.
    Elle retourne la table logiciel.
    """
    logiciel = create_table_logiciel(transform_e)

    # -- sampling des données et rescaling --
    """
    La fonction echantillonnage(transform_m, transform_h, transform_e) 
    permet de sampler les données.
    Elle retourne les dataframes samplés.
    """
    sampling = echantillonnage(transform_m, transform_h, transform_e)

    missions_transform_s= sampling[0]
    hotel_transform_s = sampling[1]
    extras_transform_s = sampling[2]

     # -- rescaling des données financières dans les tables missions et hotels -- 
    """
    La fonction rescale(missions_transform_s, hotel_transform_s) 
    permet de rescaler les données financières 
    dans les tables missions et hotels.
    Elle retourne les dataframes rescalés.
    """
    rescaling = rescale(missions_transform_s, hotel_transform_s)
    missions_transform_final = rescaling[0]
    hotel_transform_final = rescaling[1]
    extras_transform_final = extras_transform_s


    """
    La fonction join_table_logiciel_extra(extras, logiciel_df) 
    permet de créer une table de jointure entre les extras et les logiciels.
    Pour une meilleur gestion des données dans la RDS
    Elle retourne la table de jointure.
    """
    join_table = join_table_logiciel_extra(extras = extras_transform_final, logiciel_df = logiciel )

    """
    La fonction ml_transform_semaine(missions_transform_final) permet de créer 
    la table pour effectuer la prédiction.
    Elle retourne la table pour la prédiction.
    """
    # -- je créé le fichier pour la prédiction --
    ml_transform_semaine(missions_transform_final)

    """
    La table print_csv permet de créer les csv 
    pour la prédiction et de les enregistrer en local
    """
    # -- print en csv (optionnel) --
    print_csv(missions_transform=missions_transform_final,
                extra_transform=extras_transform_final,
                hotel_transform=hotel_transform_final,
                logiciel_df=logiciel,
                join_table=join_table,
                ml_table=ml_transform_semaine(missions_transform_final))

    # -- connexion au RDS et transfère des données --
    """
    La table load_data_to_rds permet de créer un engine pour se connecter au RDS.
    Elle permet également de charger les données transformées dans le RDS.
    """
    load_data_to_rds(df_missions=missions_transform_final, 
                     df_extra=extras_transform_final, 
                     df_hotel=hotel_transform_final, 
                     df_logiciel=logiciel, 
                     df_logiciel_extra=join_table)


if __name__ == "__main__":
    main()
    
  

