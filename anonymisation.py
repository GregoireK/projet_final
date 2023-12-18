# Import des libraries
import pandas as pd
# pip install Faker 
from faker import Faker
from faker.providers import company

# Import des fichiers sources
missions = pd.read_csv('00_missions.csv', sep=";")
hotel = pd.read_csv('01_hotel.csv', sep=";")
extras = pd.read_csv('02_extras.csv', sep=";")



# Compte du nombre d'hotels et d'extras
nb_hotel = hotel.shape[0]
nb_extras = extras.shape[0]
print(f"Il y a {nb_hotel} hotels et {nb_extras} extras.")

# Generation d'une instance Faker pour l'anonymisation
fake = Faker('fr_FR') 
Faker.seed(1) 

# Anonymisation extras (prénom + nom)

replace_names = {} # initialisation

for i in range(nb_extras):
  replace_names[extras.loc[i, 'nom']] = fake.unique.name() 

extras_anonymes = extras.copy() # copie pour l'anonymisation
extras_anonymes['nom'].replace(replace_names, inplace = True) # remplacement des noms par les faux noms

extras_anonymes.to_csv('extras_anonymes.csv', index=False) # export en csv

# Anonymisation des hôtels (noms + adresses)

fake.add_provider(company) # ajout du provider "company" pour les noms d'entreprises

replace_hotels = {} # initialisation

for i in range(nb_hotel):
  replace_hotels[hotel.loc[i, 'nom']] = fake.unique.company() 

replace_address = {}

for i in range(nb_hotel):
    replace_address[hotel.loc[i, 'adresse']] = fake.unique.street_address()

hotel_anonymes = hotel.copy() # copie pour l'anonymisation
hotel_anonymes['nom'].replace(replace_hotels, inplace = True) # remplacement des hotels par les faux noms d'entreprises
hotel_anonymes['adresse'].replace(replace_address, inplace = True) # remplacement des adresses par des fausses adresses

hotel_anonymes.to_csv('hotel_anonymes.csv', index=False) # export en csv

# Traitement du fichier "missions" pour retirer les noms d'hotels et d'extras

missions_anonymes = missions.copy()

missions_anonymes['hôtel'] = missions_anonymes['hôtel'].str.split(r" \(").str[0] # récupération du nom d'hôtel en enlevant l'url notion
missions_anonymes['hôtel'].replace(replace_hotels, inplace = True) # remplacement

missions_anonymes['extra'] = missions_anonymes['extra'].str.split(r" \(").str[0] # récupération de l'extra en enlevant l'url notion
missions_anonymes['extra'].replace(replace_names, inplace = True) # remplacement

missions_anonymes.drop('Propriété', axis=1, inplace=True) # suppression de la colonne "Propriété" redondante avec la colonne "hôtel"

missions_anonymes.to_csv('missions_anonymes.csv', index=False)  # export en csv

print("Tous les fichiers anonymisés ont été créés.")





