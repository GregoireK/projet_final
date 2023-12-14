# projet_final
Projet final de la formation Data analyst à JEDHA BOOTCAMP

# Contexte

La société X est une entreprise de mise en relation entre des hôtels et des extras pour des missions ponctuelles de réceptionniste. 
Elle nous a sollicité afin de réaliser 3 taches principales :
- mettre en place une base de données relationelle 
- construire un dashboard alimenté par cette BDD
- prédire le nombre de missions futur pour anticiper le besoin en extras
L'ensemble des données de l'entreprise est pour l'instant stocké dans Notion mais à terme sera hébergé dans une RDMS qui sera connectée au site web de l'entreprise.
Nous avons à notre disposition plusieurs fichiers csv extraits de Notion.

# Methodologie
Nous avons construit une infrastructure de données comprenant un Datalake pour l'hébergement des fichiers csv brut (Amazon S3) et un Datawarehouse (Amazon RDS) contenant les données transformées. La transformation a été effectuée via Python. 
Nous avons utilisé l'outil Looker Studio de Google pour la création du dashboard. 
Nous avons utilisé Dataiku pour la prédiction.

# Resultats
Nous avons proposé à la société X:
- une RDMS avec plusieurs tables permettant de stocker les informations nécessaires à l'activité de l'entreprise (cf. schéma du Logical Data Model). Cette base de donnée a servi pour la construction du dashboard mais peut etre réutilisée et enrichie lorsque le site web de la société sera déployé.
- un dashboard dans Looker avec plusieurs pages qui permet à la fois le reporting et le pilotage des opérations quotidiennes. Concernant le reporting, le dashboard inclut les KPIs utiles à l'entreprise pour évaluer sa performance passée et actuelle. Concernant le pilotage, le dashboard permet de visualiser les missions prévues / d'identifier les missions à affecter à un extra / de trouver l'extra pertinent pour une mission donnée / d'estimer le nombre de missions à venir...
- les résultats de prédiction de Dataiku ont été intégrés dans Looker pour la visualisation. Les prédictions ont été réalisé par semaine pour les 4 prochaines semaines en utilisant les données historiques (104 semaines). Le modèle le plus performant parmi ceux testés est le NPTS (MAPE 12%), c'est celui qui a été utilisé pour la prédiction. 

# Description des fichiers contenu dans le projet
- anonymisation : anonymisation des données (noms hôtels, extras, adresse) pour des raisons de confidentialité
- main : script principal qui appellent l'ensemble des autres scripts
- connexion : connexion à Amazon S3 pour la récupération des csv
- transform : transformation des données afin d'avoir un format exploitable pour la visualisation
- load_data : envoi des données transformées vers Amazon RDS
- .gitignore : pour ignorer le fichier .env (données confidentielles) lors du push vers github 
- LDM_RDS.png : image contenant le modèle data logique pour la RDS





