import pandas as pd
from arango import ArangoClient

# Goal of this program is to create a single dataframe for all data
# and sort them by product (so uuid) and data origin
# data has to be restructured

client = ArangoClient(hosts="http://localhost:8529")
sys_db = client.db("_system", username="root", password="client")


if not sys_db.has_database(("seminar2")):
    sys_db.create_database(("seminar2"))
    db = client.db("seminar2", username="root", password="client")
else:
    db = client.db("seminar2", username="root", password="client")

print("connection to database -seminar2- established")

# read data from .csv file
# Important note: files need to be in the same folder as the python file
waage_df = pd.read_csv("waage_df.csv", index_col = 0)
heiz_df = pd.read_csv("ir_temp_heiz_df.csv", index_col=0)
qual_df= pd.read_csv("ir_temp_qual_df.csv", index_col=0)
kamera_df = pd.read_csv("kamera_df.csv", index_col=0)
couple_df = pd.read_csv("thermo_couple_heiz_df.csv", index_col=0)
umgebung_df = pd.read_csv("umgebung_df.csv", index_col=0)
