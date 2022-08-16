from arango import ArangoClient

client = ArangoClient(hosts="http://localhost:8529")
sys_db = client.db("_system", username="root", password="client")

if not sys_db.has_database(("gr_test")):
    sys_db.create_database(("gr_test"))
    db = client.db("gr_test", username="root", password="client")
else:
    db = client.db("gr_test", username="root", password="client")

print("connection to database -gr_test- established")
if db.has_graph("test1"):
    db.delete_graph("test1")
graph = db.create_graph("test1")
if db.has_collection("fromnodes"):
    db.delete_collection("fromnodes")
if db.has_collection("tonodes"):
    db.delete_collection("tonodes")
#db.delete_collection("fr")
#db.delete_collection("to")
if db.has_collection("co"):
    db.delete_collection("co")
# Create vertex collections for the graph.
# Note: collections also work, if they are not created as graph.create_vertex_collection
# Therefore: normal collections can be used in graphs too

fr = db.create_collection("fromnodes")
to = db.create_collection("tonodes")

# Create an edge definition (relation) for the graph.
edges = graph.create_edge_definition(
    edge_collection="co",
    from_vertex_collections=["fromnodes"],
    to_vertex_collections=["tonodes"]
)

# Insert vertex documents into "students" (from) vertex collection.
fr.insert({"_key": "01", "full_name": "Anna Smith"})
fr.insert({"_key": "02", "full_name": "Jake Clark"})
fr.insert({"_key": "03", "full_name": "Lisa Jones"})

# Insert vertex documents into "lectures" (to) vertex collection.
to.insert({"_key": "MAT101", "title": "Calculus"})
to.insert({"_key": "STA101", "title": "Statistics"})
to.insert({"_key": "CSC101", "title": "Algorithms"})

# Insert edge documents into "register" edge collection.
edges.insert({"_from": "fromnodes/01", "_to": "tonodes/MAT101"})
edges.insert({"_from": "fromnodes/01", "_to": "tonodes/STA101"})
edges.insert({"_from": "fromnodes/01", "_to": "tonodes/CSC101"})
edges.insert({"_from": "fromnodes/02", "_to": "tonodes/MAT101"})
edges.insert({"_from": "fromnodes/02", "_to": "tonodes/STA101"})
edges.insert({"_from": "fromnodes/03", "_to": "tonodes/CSC101"})

# Traverse the graph in outbound direction, breadth-first.
#result = graph.traverse(
#    start_vertex="students/01",
#    direction="outbound",
#    strategy="breadthfirst"
#)


