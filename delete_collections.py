import pandas as pd
from arango import ArangoClient

client = ArangoClient(hosts="http://localhost:8529")
sys_db = client.db("_system", username="root", password="client")

if not sys_db.has_database(("seminar2")):
    sys_db.create_database(("seminar2"))
    db = client.db("seminar2", username="root", password="client")
else:
    db = client.db("seminar2", username="root", password="client")
c = 0
print("connection to database -seminar2- established")

#print("List all collections of database -seminar2-")
#print(db.collections())
d = list(())
if db.has_collection("umgebung"):
    db.delete_collection("umgebung")
    c += 1
    d.append("umgebung")
if db.has_collection("kamera"):
    db.delete_collection("kamera")
    d.append("kamera")
    c += 1
if db.has_collection("qual"):
    db.delete_collection("qual")
    c += 1
    d.append("qual")
if db.has_collection("heiz"):
    db.delete_collection("heiz")
    c += 1
    d.append("heiz")
if db.has_collection("waage"):
    db.delete_collection("waage")
    c += 1
    d.append("waage")
if db.has_collection("couple"):
    db.delete_collection("couple")
    c += 1
    d.append("couple")
if db.has_collection("produkt"):
    db.delete_collection("produkt")
    c += 1
    d.append("produkt")
print("\n" +  str(c)+" collections deleted")
print("deleted collections are:")
print(d)