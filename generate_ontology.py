import rdflib
import random
from rdflib.namespace import RDF, RDFS, OWL
from pyshacl import ShapesGraph
from pathlib import Path

# --------------------
# Config
# --------------------
dataset_uri = "source/Datasets/EnDe-Lite50(without_Ontology).ttl"
shape30_uri = "source/ShapesGraphs/Shape_30.ttl"
ontology_uri = "source/Ontologies/dbpedia_ontology.owl"
modified_ontology_uri = "source/Ontologies/dbpedia_ontology_inflated.ttl"

# Adjustable variable for top classes (you can modify this)
TOP_N_CLASSES = 20
OFFSET=5

# --------------------
# Load graphs
# --------------------
print("Loading data graph...")
data_graph = rdflib.Graph()
data_graph.parse(dataset_uri, format="turtle")

print("Loading ontology...")
ontology_graph = rdflib.Graph()
ontology_graph.parse(ontology_uri, format="xml")  

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

shape30_targets = get_target_classes(shape30_shapes)

# --------------------
# Identify new target classes
# --------------------
new_targets = shape30_targets
print(f"Found {len(new_targets)} target classes in Shape_30.")

# --------------------
# Count instances in data graph
# --------------------
def count_instances(cls, include_subclasses=False) -> int:
    cls_iri = rdflib.URIRef(cls)
    if include_subclasses:
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
# Get top N frequent classes
# --------------------
def get_top_classes(n=TOP_N_CLASSES, offset=OFFSET):
    counts = {}
    for s, _, c in data_graph.triples((None, RDF.type, None)):
        if isinstance(c, rdflib.URIRef):
            counts[str(c)] = counts.get(str(c), 0) + 1

    sorted_classes = sorted(
        counts.items(),
        key=lambda item: item[1],
        reverse=True
    )

    return [c for c, _ in sorted_classes[offset:offset + n]]


top_classes = get_top_classes(n=TOP_N_CLASSES)
print(f"Top {TOP_N_CLASSES} frequent classes in the data graph (sorted by instance count):")
for cls in top_classes:
    print(cls)

# --------------------
# Randomly adjust ontology
# --------------------
def random_inflate_ontology(target_classes, frequent_classes, ontology_graph):
    # Randomly choose classes to adjust as subclasses/equivalents/sameAs
    for target in target_classes:
        for freq_class in frequent_classes:
            # Choose random relation
            relation = random.choice([RDFS.subClassOf, OWL.equivalentClass, OWL.sameAs])
            # Add to ontology
            ontology_graph.add((rdflib.URIRef(freq_class), relation, rdflib.URIRef(target)))
            # Add self-relation (frequency class as subclass/equivalent/sameAs of itself)
            ontology_graph.add((rdflib.URIRef(freq_class), relation, rdflib.URIRef(freq_class)))
    
    return ontology_graph

# --------------------
# Apply random inflation
# --------------------
ontology_graph = random_inflate_ontology(new_targets, top_classes, ontology_graph)

# --------------------
# Save the updated ontology
# --------------------
print(f"Saving the modified ontology to {modified_ontology_uri}...")
ontology_graph.serialize(destination=modified_ontology_uri, format="turtle")
print("Ontology saved.")

# --------------------
# Verify changes
# --------------------
def verify_changes():
    missing_classes = []
    for freq_class in top_classes:
        for target in new_targets:
            relation_check = (rdflib.URIRef(freq_class), None, rdflib.URIRef(target))
            if not any(ontology_graph.triples(relation_check)):
                missing_classes.append((freq_class, target))

    if missing_classes:
        print("❌ Missing relations for the following pairs:")
        for c, t in missing_classes:
            print(f"  - {c} → {t}")
    else:
        print("✅ All expected relations are present.")

verify_changes()
