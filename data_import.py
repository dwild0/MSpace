import pandas as pd
from arango import ArangoClient
import time
from datetime import timedelta
# this function creates documents in the passed collection of
# name_string. It uses the entries of df dataframe

def create_coll_from_df(df, name_string):
    if db.has_collection(name_string):
        print("Collection " + name_string +" already exists. Would you like to delete it and create an empty one?")
        if input("[y/n]?  ")=="y":
            db.delete_collection(name_string)
            db.create_collection(name_string)
            print("collection created from scratch")
            print("y")
        else:
            return err_existing_col
    else:
        db.create_collection(name_string)
    print("check 1")
    col = db.collection(name_string)
    print("check: passed coll assignment")
    for i in df.index:
        col.insert(get_object(df, i, name_string))
    return 1

# This function creates a dictionary object, that is passed to the
# insert function
# It is necessary.because the length of this dictionary varies
# with the dataframes

def get_object (df, row, collection_name):
    #col_count = len(df.columns)
    key_string = collection_name[0] + "M" + str(row)
    obj = {"_key": key_string}
    for i in df.columns:
        obj[i] = df.loc[row, i]
    return obj


## Error messages
err_existing_col = "Error creating collection, collection already exists"

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

# #########
# Note: timestamp objects in dataframes are from type string
# these timestamps can't be compared or used for computations
# without further preparation
# BUT: converting them into datetime objects with pd.to_datetime
# will raise an error while inserting them into a database
# collection. That is, because the .json integration of datetime
# objects is not supported at this state
# #########

# TypeError: Object of type int64 is not JSON serializable -->
umgebung_df["env_iaq_index"]=umgebung_df["env_iaq_index"].astype(float)

create_coll_from_df(waage_df, "waage")
create_coll_from_df(heiz_df, "heiz")
create_coll_from_df(qual_df, "qual")
create_coll_from_df(kamera_df, "kamera")
create_coll_from_df(couple_df, "couple")
create_coll_from_df(umgebung_df, "umgebung")




