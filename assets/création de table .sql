CREATE TABLE hotel (
    hotel_id VARCHAR PRIMARY KEY,
    hotel_nom VARCHAR NOT NULL,
    hotel_adresse VARCHAR NOT NULL,
    zipcode INT NOT NULL,
    hotel_ville VARCHAR NOT NULL,
    hotel_logiciel VARCHAR NOT NULL,
    nbre_chambre INT,
    tarif_horaire INT NOT NULL,
    MAJ_procedures VARCHAR, 
    status VARCHAR,
    last_contact TIMESTAMP,
    notes VARCHAR,
    date_de_creation TIMESTAMP NOT NULL
);



CREATE TABLE extra (
    extra_id VARCHAR PRIMARY KEY,
    nom VARCHAR NOT NULL,
    logiciel VARCHAR,
    statuts VARCHAR,
    vacance_debut TIMESTAMP,
    vacance_fin TIMESTAMP,
    disponibilités VARCHAR,
    date_de_creation TIMESTAMP,
    nbre_mission INT
    
   
);

CREATE TABLE missions (
    mission_id SERIAL PRIMARY KEY,
    hotel_id VARCHAR REFERENCES hotel(hotel_id) NOT NULL,
    extra_id VARCHAR REFERENCES extra(extra_id) NOT NULL,
    tarif_urgence INT, 
    tarif_horaire INT NOT NULL, 
    facture VARCHAR, 
    texte VARCHAR, 
    date_debut TIMESTAMP NOT NULL,
    date_fin TIMESTAMP NOT NULL,
    duree_mission INT NOT NULL, 
    urgent BOOLEAN, 
    annulation BOOLEAN,
    heures_supp INT,
    Total_HT INT,
    Total_TTC INT,
    extra_salaire INT,
    bénéfice INT,
    CONSTRAINT fk_hotel FOREIGN KEY (hotel_id) REFERENCES hotel(hotel_id),
    CONSTRAINT fk_extra FOREIGN KEY (extra_id) REFERENCES extra(extra_id)
);

CREATE TABLE logiciel(
        logiciel_id VARCHAR PRIMARY KEY,
        logiciel_name VARCHAR NOT NULL
); 

CREATE TABLE logiciel_extra(
        extra_id VARCHAR REFERENCES extra(extra_id) NOT NULL,
        logiciel_id VARCHAR REFERENCES logiciel(logiciel_id) NOT NULL,
        CONSTRAINT fk_extra FOREIGN KEY (extra_id) REFERENCES extra(extra_id),
        CONSTRAINT fk_logiciel FOREIGN KEY (logiciel_id) REFERENCES logiciel(logiciel_id)
);
        
  

DROP TABLE missions;
DROP TABLE hotel;
DROP TABLE extra;
DROP TABLE logiciel;

SELECT * FROM hotel
SELECT * FROM missions

SELECT * FROM extra;
SELECT * FROM logiciel;
select * FROM logiciel_extra


