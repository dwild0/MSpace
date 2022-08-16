import pandas as pd
from arango import ArangoClient

client = ArangoClient(hosts="http://localhost:8529")
sys_db = client.db("_system", username="root", password="client")


if not sys_db.has_database(("seminar2")):
    sys_db.create_database(("seminar2"))
    db = client.db("seminar2", username="root", password="client")
else:
    db = client.db("seminar2", username="root", password="client")

print("connection to database -seminar2- established\n")

waage_df = pd.read_csv("waage_df.csv", index_col=0)

uuid_series=waage_df["uuid"].value_counts()

print(uuid_series.head())
print("\ntotal number of uuids: " + str(len(uuid_series)))

if db.has_collection("produkt"):
    db.delete_collection("produkt")
    print("collection deleted")
    db.create_collection("produkt")
    produkt=db.collection("produkt")
    print("new empty collection created")
else:
    print("collection doesn't exist. Generating a new one")
    db.create_collection("produkt")
    produkt=db.collection("produkt")

for i in range(len(uuid_series)):
    key_string = "P"+str(i)
    name_string= "Produkt_" + str(i)
    produkt.insert({"_key":key_string, "uuid": uuid_series.index[i], "name": "produkt_"+str(i) })