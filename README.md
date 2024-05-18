1. Se connecter à la VM :
2. La Vm est sur le lien suivant : https://portal.azure.com/#@ynov.com/resource/subscriptions/40e83b3d-aed4-402f-a938-6c35c127afd6/resourceGroups/ml_ops/providers/Microsoft.Compute/virtualMachines/mlOpsVM/overview
3. Info sur la VM : @IP : 13.82.178.239
4.   Avoir la clé d'authentification dans le ssh (pas besoin normalement)

7.   Dedans y a le dossier airflow
8.     dedans on lance le docker-compose up
9.     Se connecter à airflow : http://13.82.178.239:8080/home
10.     Mot de passe airflow : airflow, airflow
11.     Se connecter Mlflow : http://13.82.178.239:5001/

12. Ce qui reste a faire : modifier le code pour qu'il soit adapter aux nouvelles data


