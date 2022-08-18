import pandas as pd
from arango import ArangoClient

# Goal of this program is to create a single dataframe for all data
# and sort them by product (so uuid) and data origin
# data has to be restructured

client = ArangoClient(hosts="http://localhost:8529")
sys_db = client.db("_system", username="root", password="client")

if not sys_db.has_database("seminar2"):
    sys_db.create_database("seminar2")
    db = client.db("seminar2", username="root", password="client")
else:
    db = client.db("seminar2", username="root", password="client")

print("connection to database -seminar2- established")

# read data from .csv file
# Important note: files need to be in the same folder as the python file
waage_df = pd.read_csv("waage_df.csv", index_col=0)
heiz_df = pd.read_csv("ir_temp_heiz_df.csv", index_col=0)
qual_df = pd.read_csv("ir_temp_qual_df.csv", index_col=0)
kamera_df = pd.read_csv("kamera_df.csv", index_col=0)
couple_df = pd.read_csv("thermo_couple_heiz_df.csv", index_col=0)
umgebung_df = pd.read_csv("umgebung_df.csv", index_col=0)

waage_df.insert(loc=3, column="source", value="waage")
heiz_df.insert(loc=9, column="source", value="heiz")
qual_df.insert(loc=9, column="source", value="qual")
kamera_df.insert(loc=4, column="source", value="kamera")
couple_df.insert(loc=4, column="source", value="couple")
umgebung_df.insert(loc=6, column="source", value="umgebung")
# Format
# waage: -uuid, -timestamp_qual_start, -v1
# heiz: uuid, timestamp_prod_start, -timestamp_start, -v1, -v2, timestamp_prod_end, timestamp_end, start_date, end_date
# qual: uuid, timestamp_qual_start, -timestamp_start, -v1, -v2, timestamp_qual_end, timestamp_end, start_date, end_date
# kamera: -uuid, -timestamp_qual_start, -v1, -v2
# couple: -uuid, -timestamp_prod_start, -v1, -v2
# umgebung: -uuid, -timestamp_start, -v1, -v2, -v3, -v4

waage_df.rename(columns={"qual_weight": "value1"})
heiz_df.rename(columns={"prod_obj_temp": "value1", "prod_amb_temp": "value2"})
qual_df.rename(columns={"qual_obj_temp": "value1", "qual_amb_temp": "value2"})
kamera_df.rename(columns={"qual_height": "value1", "qual_color": "value2"})
couple_df.rename(columns={"prod_plt_temp": "value1"})
umgebung_df.rename(columns={"env_temperature": "value1", "env_humidity": "value2"})
umgebung_df.rename(columns={"env_iaq_index": "value3", "env_pressure": "value4"})

header_list = ['uuid', 'timestamp_start', "timestamp_prod_start", 'timestamp_qual_start', 'value1', "value2"]
header_list.extend(["value3", "value4", "timestamp_end", "timestamp_prod_end", "timestamp_qual_end", "start_date"])
header_list.extend(["end_date", "source"])

# df = df.reindex(columns = header_list)
fwaage = waage_df.reindex(columns=header_list)
fheiz = heiz_df.reindex(columns=header_list)
fqual = qual_df.reindex(columns=header_list)
fkamera = kamera_df.reindex(columns=header_list)
fcouple = couple_df.reindex(columns=header_list)
fumgebung = umgebung_df.reindex(columns=header_list)

df = pd.concat([fwaage, fheiz, fqual, fkamera, fcouple, fumgebung])

print("columns are:")
print(df.columns)
print("shape of all_data: ")
print(df.shape)
