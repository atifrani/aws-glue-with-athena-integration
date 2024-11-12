# Workshop : Intégration d'Athena avec AWS Glue via AWS CLI

## Objectif
Ce workshop vise à démontrer comment intégrer Amazon Athena avec AWS Glue en utilisant uniquement l’AWS CLI. Les étapes incluent :
1. Hébergement de fichiers CSV et JSON sur Amazon S3
2. Configuration des crawlers AWS Glue pour cataloguer les données
3. Création d’un job ETL Glue pour transformer les données
5. Automatisation du pipeline avec AWS Glue Workflow
6. Surveillance de l’exécution avec CloudWatch

## Prérequis
- Compte AWS avec les autorisations nécessaires
- AWS CLI installé et configuré (`aws configure` pour définir l’access key, secret key, région)
- Fichiers CSV et JSON prêts pour le chargement

## Étapes du Workshop

### Étape 1 : Hébergement des fichiers sur Amazon S3
1. Créer un bucket S3 via la console ou le CLI :
```bash
aws s3api create-bucket --bucket airbnb-data-workshop --region eu-west-1
```

2. Charger les fichiers CSV et JSON via la console ou le CLI:
```bash
aws s3 cp ./csv/ s3://airbnb-data-workshop/csv_data/ --recursive
aws s3 cp ./json/ s3://airbnb-data-workshop/json_data/ --recursive
```

### Étape 2 : Configuration des Crawlers AWS Glue
1. Créer un fichier database-input.json :
```json
{
  "DatabaseInput": {
    "Name": "workshop_db"
  }
}
```

2. Exécuter la commande pour créer la base de données :
```bash
aws glue create-database --cli-input-json file://database-input.json
```

3. Créer un rôle IAM pour Glue avec les autorisations nécessaires, en utilisant glue-trust-policy.json :
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

```bash
aws iam create-role --role-name AWSGlueServiceRole-kris-workshop --assume-role-policy-document file://glue-trust-policy.json
```

4. Creer une politique via mon fichier json et l'attacher au role AWSGlueServiceRole-kris-workshop:
AWSGlueServiceRole-kris-workshop-policy.json
```json
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Effect": "Allow",
          "Action": [
              "s3:GetObject",
              "s3:PutObject",
              "s3:PutObjectAcl"
          ],
          "Resource": [
              "arn:aws:s3:::airbnb-data-workshop/csv_data/*",
              "arn:aws:s3:::airbnb-data-workshop/glue_transformed_data/*"
          ]
      },
      {
          "Effect": "Allow",
          "Action": "s3:ListBucket",
          "Resource": "arn:aws:s3:::airbnb-data-workshop",
          "Condition": {
              "StringLike": {
                  "s3:prefix": [
                      "csv_data/*",
                      "glue_transformed_data/*"
                  ]
              }
          }
      }
  ]
}
```

```bash
$policyArn = (aws iam create-policy `
    --policy-name AWSGlueServiceRole-kris-workshop-policy `
    --policy-document file://AWSGlueServiceRole-kris-workshop-policy.json |
    ConvertFrom-Json).Policy.Arn
```

```bash
aws iam attach-role-policy `
    --role-name AWSGlueServiceRole-kris-workshop `
    --policy-arn $policyArn
```

5. Attacher le reste des politiques nécessaires à l'exécution de notre job via la console en les recherchant:
  - AmazonAthenaFullAccess
  - AmazonS3FullAccess
  - AWSGlueConsoleFullAccess
  - AWSGlueServiceRole

6. Créer un crawler pour les fichiers CSV, en utilisant csv-crawler-config.json :
```json
{
  "Name": "csv_crawler",
  "Role": "AWSGlueServiceRole-kris-workshop",
  "DatabaseName": "workshop_db",
  "Targets": {
    "S3Targets": [
      {
        "Path": "s3://airbnb-data-workshop/csv_data/"
      }
    ]
  }
}
```

```bash
aws glue create-crawler --cli-input-json file://csv-crawler-config.json
```

7. Créer un crawler pour les fichiers JSON, en utilisant json-crawler-config.json :
```json
{
  "Name": "json_crawler",
  "Role": "AWSGlueServiceRole-kris-workshop",
  "DatabaseName": "workshop_db",
  "Targets": {
    "S3Targets": [
      {
        "Path": "s3://airbnb-data-workshop/json_data/"
      }
    ]
  }
}
```

```bash
aws glue create-crawler --cli-input-json file://json-crawler-config.json
```

8. Exécuter les crawlers :
```bash
aws glue start-crawler --name csv_crawler
aws glue start-crawler --name json_crawler
```

### Étape 3 : Transformation des données via un job ETL AWS Glue
1. Créer un script ETL en Python (par exemple spark-job.py) et le stocker dans un bucket S3.
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from S3 into a Glue DynamicFrame
data_source = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://airbnb-data-workshop/csv_data/raw_listings.csv"], "recurse": True},
    transformation_ctx="data_source"
)

# Change the schema of the DynamicFrame to align with Spark DataFrame expectations
data_transform = ApplyMapping.apply(
    frame=data_source,
    mappings=[
        ("id", "string", "listing_id", "string"),
        ("listing_url", "string", "listing_url", "string"),
        ("name", "string", "listing_name", "string"),
        ("room_type", "string", "room_type", "string"),
        ("minimum_nights", "string", "minimum_nights", "string"),
        ("host_id", "string", "host_id", "string"),
        ("price", "string", "price_str", "string"),
        ("created_at", "string", "created_at", "string"),
        ("updated_at", "string", "updated_at", "string")
    ],
    transformation_ctx="data_transform"
)

# Convert the DynamicFrame to a Spark DataFrame
csv_df = data_transform.toDF()

# Apply the transformations
transformed_df = csv_df.select(
    F.col("listing_id"),
    F.col("listing_name"),
    F.col("room_type"),
    F.when(F.col("minimum_nights") == "0", "1").otherwise(F.col("minimum_nights")).alias("minimum_nights"),
    F.col("host_id"),
    F.regexp_replace(F.col("price_str"), "\\$", "").cast("double").alias("price"),
    F.to_timestamp(F.regexp_replace(F.regexp_replace(F.col("created_at"), "T", " "), "Z", ""), "yyyy-MM-dd HH:mm:ss").alias("created_at"),
    F.to_timestamp(F.regexp_replace(F.regexp_replace(F.col("updated_at"), "T", " "), "Z", ""), "yyyy-MM-dd HH:mm:ss").alias("updated_at")  # Updated transformation for updated_at
)

# Convert the transformed Spark DataFrame back to a Glue DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dynamic_frame")

# Write the transformed DynamicFrame to S3
data_target = glueContext.getSink(
    path="s3://airbnb-data-workshop/glue_transformed_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="data_target"
)
data_target.setCatalogInfo(catalogDatabase="workshop_db", catalogTableName="transformed_listings")
data_target.setFormat("glueparquet", compression="uncompressed")
data_target.writeFrame(transformed_dynamic_frame)

# Commit the job
job.commit()
```

2. Copiez votre job py-Spark sur S3:
```bash
aws s3 cp spark-job.py s3://airbnb-data-workshop/pyspark-script/spark-job.py
```

3. Créer le job Glue, en utilisant glue-job-config.json :
```json
{
  "Name": "ETL-glue-csv-job",
  "Role": "AWSGlueServiceRole-kris-workshop",
  "Command": {
    "Name": "glueetl",
    "ScriptLocation": "s3://airbnb-data-workshop/pyspark-script/spark-job.py"
  }
}
```

```bash
aws glue create-job --cli-input-json file://glue-job-config.json
```

4. Charger vos dépendances dans le meme repertoire que votre job sur s3
AWS Glue vous permet de charger vos bibliothèques Python via un fichier ZIP. Voici comment procéder :
a. Créer un dossier pour votre projet :
```bash
mkdir my_glue_job
cd my_glue_job
```
b. Ajouter votre code : Copiez votre script Python (par exemple spark-job.py) dans ce dossier.

c. Créer votre requirements.txt et ajoutez-y les dépendances nécessaires à l'exécution du job
requirements.txt

d. Installer vos dépendances dans le dossier : Utilisez l'option -t pour installer les dépendances dans le dossier de votre projet :
```bash
pip install -r requirements.txt -t .
```

e. Créer un fichier ZIP : Zippez tous les fichiers pour créer votre package de déploiement :
```bash
zip -r my_glue_job.zip .
```

f. Charger le package ZIP sur S3 :
Utiliser AWS CLI pour télécharger le fichier ZIP vers S3 : Assurez-vous que vous avez AWS CLI configuré avec les permissions appropriées, puis exécutez :
```bash
aws s3 cp my_glue_job.zip s3://airbnb-data-workshop/pyspark-script/my_glue_job.zip
```

5. Exécuter le job Glue
Utilisez la commande aws glue start-job-run.
```bash
aws glue start-job-run --job-name "ETL-glue-csv-job"
```

6. Monitor the Glue Job:
Vérifier le statut du job exécuté. Remplacez YOUR_JOB_RUN_ID par le run ID du job retourné.
```bash
aws glue get-job-run --job-name "ETL-glue-csv-job" --run-id YOUR_JOB_RUN_ID
```

### Etape 4 : Interroger les données transformées dans Athena via PowerShell
1. Créer le fichier de requête .sql :
Utilisez PowerShell pour écrire la requête dans un fichier .sql. Cet exemple enregistre la requête dans un fichier nommé query.sql :
```bash
$requete = "SELECT * FROM workshop_db.transformed_listings LIMIT 10;"
$requete | Out-File -FilePath "./query.sql" -Encoding UTF8
```

2. Exécuter la requête dans Athena en utilisant l’AWS CLI :
Passez le contenu du fichier .sql comme paramètre de la commande start-query-execution dans Athena :
```bash
$queryString = Get-Content -Path "./query.sql" -Raw
aws athena start-query-execution `
    --query-string "$queryString" `
    --query-execution-context Database=workshop_db `
    --result-configuration OutputLocation="s3://airbnb-data-workshop/athena_results/"
```
Après exécution, Athena renvoie un QueryExecutionId à noter pour récupérer les résultats.

Récupérer les résultats de la requête :
Utilisez le QueryExecutionId pour obtenir les résultats. Remplacez YOUR_QUERY_EXECUTION_ID par l’ID renvoyé :
```bash
aws athena get-query-results --query-execution-id YOUR_QUERY_EXECUTION_ID
```

Cette méthode permet de stocker la requête dans un fichier .sql séparé, facilitant ainsi la modification et la réutilisation de la requête.
