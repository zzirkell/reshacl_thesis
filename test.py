import rdflib
from pyshacl import ShapesGraph
from pathlib import Path

# --------------------
# Config
# --------------------
dataset_uri = "source/Datasets/EnDe-Lite50(without_Ontology).ttl"
expanded_shapes_uri = "source/ShapesGraphs/expanded_shapes.ttl"
shape30_uri = "source/ShapesGraphs/Shape_30.ttl"
ontology_uri = "source/dbpedia_ontology.owl"

# --------------------
# Load graphs
# --------------------
print("Loading data graph...")
data_graph = rdflib.Graph()
data_graph.parse(dataset_uri, format="turtle")

print("Loading ontology...")
ontology_graph = rdflib.Graph()
ontology_graph.parse(ontology_uri, format="xml")  # OWL is usually RDF/XML

print("Loading expanded shapes graph...")
expanded_shapes_graph = rdflib.Graph()
expanded_shapes_graph.parse(expanded_shapes_uri, format="turtle")
expanded_shapes = ShapesGraph(expanded_shapes_graph)

print("Loading Shape_30 graph...")
shape30_graph = rdflib.Graph()
shape30_graph.parse(shape30_uri, format="turtle")
shape30_shapes = ShapesGraph(shape30_graph)

# --------------------
# Extract target classes
# --------------------
def get_target_classes(shapes_graph):
    t_classes = set()
    for shape in shapes_graph.shapes:
        for t in shape.target_classes():  # call method
            if isinstance(t, rdflib.URIRef):
                t_classes.add(t)
    return t_classes

expanded_targets = get_target_classes(expanded_shapes)
shape30_targets = get_target_classes(shape30_shapes)

# --------------------
# Identify new classes
# --------------------
new_targets = expanded_targets - shape30_targets
print(f"Found {len(new_targets)} target classes in expanded_shapes not in Shape_30.")

# --------------------
# Count instances in data graph
# --------------------
def count_instances(cls, include_subclasses=False) -> int:
    cls_iri = rdflib.URIRef(cls)
    if include_subclasses:
        # Use recursive path to count subclasses as well
        query = f"""
        SELECT (COUNT(?x) AS ?num)
        WHERE {{
          ?x rdf:type/rdfs:subClassOf* <{cls_iri}> .  # Recursive subclass path
        }}
        """
    else:
        query = f"""
        SELECT (COUNT(?x) AS ?num)
        WHERE {{
          ?x rdf:type <{cls_iri}> .  # Direct type only
        }}
        """
    qres = data_graph.query(query)
    return int(list(qres)[0]["num"])

# --------------------
# Count and print instances
# --------------------
print("Counting instances of new target classes...")
counts = {}
total_instances = 0

for cls in new_targets:
    num = count_instances(cls, include_subclasses=True)  # Count subclasses too
    counts[str(cls)] = num
    total_instances += num
    print(f"Class {cls}: {num} instances")

print(f"Total instances across all new target classes: {total_instances}")
print("Done.")
