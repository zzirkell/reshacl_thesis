from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF, RDFS

dataset_uri = "source/Datasets/EnDe-Lite50(without_Ontology).ttl"
shape30_uri = "source/ShapesGraphs/Shape_30.ttl"
inflated_ontology_uri = "source/dbpedia_ontology_inflated.ttl"

SH = Namespace("http://www.w3.org/ns/shacl#")

data = Graph().parse(dataset_uri, format="turtle")
shape30 = Graph().parse(shape30_uri, format="turtle")
ont = Graph().parse(inflated_ontology_uri, format="turtle")

targets = set(tc for _, _, tc in shape30.triples((None, SH.targetClass, None)))

# Get direct subclasses of dbo:Road in the inflated ontology (the ones we injected)
road = URIRef("http://dbpedia.org/ontology/Road")
expanded = set(targets) | set(ont.subjects(RDFS.subClassOf, road))

nodes = set()
for s, _, c in data.triples((None, RDF.type, None)):
    if c in expanded:
        nodes.add(s)

print("Targets in Shape_30:", len(targets))
print("Expanded targets (targets + subclasses of dbo:Road):", len(expanded))
print("Estimated targeted nodes after inflation:", len(nodes))
