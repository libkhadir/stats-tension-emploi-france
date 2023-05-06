from pyspark.sql import SparkSession, SQLContext
import requests
import json
from json.decoder import JSONDecodeError
import re
import os

def get_spark_session(env, appName):
    return SparkSession. \
        builder. \
        master(env). \
        appName(appName). \
        getOrCreate()

def connect():
    body = {"grant_type": os.environ.get('GRANT_TYPE', ''),
            "client_id": os.environ.get('CLIENT_ID', ''),
            "client_secret": os.environ.get('SECRET', ''),
            "scope": os.environ.get('SCOPE', '')}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    url = "https://entreprise.pole-emploi.fr/connexion/oauth2/access_token?realm=/partenaire"
    response = requests.post(url, data=body, headers=headers)
    return response.json()

def get_communes(token: str):
    url="https://api.emploi-store.fr/partenaire/offresdemploi/v2/referentiel/communes"
    headers = {"Content-type": "application/json",
               "Authorization": "Bearer {}".format(token)}
    response = requests.get(url, headers=headers)
    return response.json()

def get_jobs(token: str, commune: str):
    url = "https://api.emploi-store.fr/partenaire/offresdemploi/v2/offres/search?offresManqueCandidats=true&commune={}".format(commune)
    headers = {"Content-type": "application/json",
               "Authorization": "Bearer {}".format(token)}
    response = requests.get(url, headers=headers)
    filtres = []
    jsonResponse = {}
    try:
        jsonResponse = response.json()
    except JSONDecodeError:
        print("nothing found for", commune)
    if response.status_code != 200 and "filtresPossibles" in jsonResponse:
        for f in response.json()['filtresPossibles']:
            if f['filtre'] == "typeContrat":
                for a in f['agregation']:
                    filtres.append(a['valeurPossible'])
    print("found filters", filtres)
    if len(filtres) == 0 and "resultats" in jsonResponse:
        with open("result{}.json".format(commune), "w") as out:
            json.dump(jsonResponse['resultats'], out)
    elif "resultats" in jsonResponse:
        for f in filtres:
            url = "https://api.emploi-store.fr/partenaire/offresdemploi/v2/offres/search?offresManqueCandidats=true&commune={}&typeContrat={}".format(
                commune, f)
            response = requests.get(url, headers=headers)
            with open("result{}-{}.json".format(commune, f), "w") as out:
                json.dump(response.json()['resultats'], out)

if __name__ == "__main__":
    spark = get_spark_session('local', 'stats tension app')
    spark.sql('SELECT current_date').show()

    jsonToken = connect()
    communes = get_communes(jsonToken['access_token'])
    for c in communes:
        if re.match("^LILLE$", c["libelle"]):
            get_jobs(jsonToken['access_token'], c['code'])

    spark.read.json("result*.json") \
        .createOrReplaceTempView("jobs_view")
    sql_c = SQLContext(spark.sparkContext)
    sql_c.sql("select appellationlibelle as intitule, origineOffre.urlOrigine as lien_postuler from jobs_view order by appellationlibelle")\
        .write\
        .option("header", "true")\
        .option("delimiter", ",")\
        .csv("./export.csv")

    spark.stop()
