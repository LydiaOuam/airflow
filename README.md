1. Se connecter à la VM :
2. La Vm est sur le lien suivant : https://portal.azure.com/#@ynov.com/resource/subscriptions/40e83b3d-aed4-402f-a938-6c35c127afd6/resourceGroups/ml_ops/providers/Microsoft.Compute/virtualMachines/mlOpsVM/overview
3. Info sur la VM : @IP : 13.82.178.239
4. Dedans y a le dossier airflow
5. Lancer la commande suivante pour le docker : docker-compose up
6. Se connecter à airflow : http://13.82.178.239:8080/home
7. Mot de passe airflow : airflow, airflow
8. Se connecter Mlflow : http://13.82.178.239:5001/
9. Se connecter a la base de donnée sur Postgre : db_ademe, mot de passe : Passe2024#
psql "--host=postgres-mlops.postgres.database.azure.com" "--port=5432" "--dbname=db_ademe" "--username=ouamrane_lydia2022" "--set=sslmode=require"


## Membre Du Group 
### AIROUCHE Kafia 
### OULMOU Kenza
### BELGHITI Boutaina
### OUAMRANE Lydia


