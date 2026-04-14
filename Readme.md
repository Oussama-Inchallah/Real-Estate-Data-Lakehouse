# 📝 Table of Contents

1. [Architecture](#architecture)
2. [Description du Projet](#description-du-projet)
3. [ETL Pipeline (Medallion)](#etl)
4. [Business Intelligence](#bi)
5. [Galerie d’implémentation](#galerie-dimplementation)
6. [Contact](#contact)

## Architecture

![Image 1](https://github.com/Oussama-Inchallah/Real-Estate-Data-Lakehouse/blob/main/Architecture/medallion_architecture%20.jpg)  

## Description du Projet

Ce projet est un lac de données (data lakehouse) déployé sur AWS. Il comprend :  

- Un stockage S3 pour le landing des données brutes  
- Des travaux AWS Glue en PySpark qui mettent en œuvre l'architecture médallion (Bronze/Silver/Gold)  
- Des requêtes effectuées avec Athena  
- Des analyses de données via Power BI.

<a id="etl"></a>
## ⚙️​ ETL Pipeline (Medallion)

### Bronze  

- Travaux AWS Glue pour le chargement des données brutes.  
- Enforcement du schéma et qualité des données.

### Silver  

- **Transformation** des données avec PySpark.
- **Filtrage** des enregistrements non conformes ou non pertinents.  

### Gold  

- Agrégation et partitionnement des données dans des formats optimisés.
- Préparation finale pour la consommation par les dashboards et BI.  
- Vérifications de qualité avancées.

<a id="bi"></a>
## 📊 Business Intelligence

![Dashboard Image 1](https://github.com/Oussama-Inchallah/Real-Estate-Data-Lakehouse/blob/main/Dashboard-%20Power%20BI/Power%20BI%20N1.jpg)  
  
![Dashboard Image 2](https://github.com/Oussama-Inchallah/Real-Estate-Data-Lakehouse/blob/main/Dashboard-%20Power%20BI/Power%20BI%20N2.jpg)  

<a id="galerie-dimplementation"></a>
## 📸 Galerie d’implémentation

![Glue Jobs](https://github.com/Oussama-Inchallah/Real-Estate-Data-Lakehouse/blob/main/Implementation%20Gallery/aws%20jobs%20image.jpg)
![S3 processed data](https://github.com/Oussama-Inchallah/Real-Estate-Data-Lakehouse/blob/main/Implementation%20Gallery/S3%20processed%20data.jpg)
![S3 gold data](https://github.com/Oussama-Inchallah/Real-Estate-Data-Lakehouse/blob/main/Implementation%20Gallery/S3%20gold%20data%20partioned%20by%20year.jpg)
![Amazon Athena](https://github.com/Oussama-Inchallah/Real-Estate-Data-Lakehouse/blob/main/Implementation%20Gallery/amazon%20athena%20.jpg)

<a id="contact"></a>
## 📨 Contact

[LinkedIn](https://www.linkedin.com/in/oussama-inchallah/)
