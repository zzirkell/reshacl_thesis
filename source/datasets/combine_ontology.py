from rdflib import Graph

# Create a new RDF graph
g = Graph()

# Load the ontology file
g.parse("dbpedia_ontology.owl", format="xml")

# Load the TTL data file
g.parse("EnDe-Lite50(without_Ontology).ttl", format="turtle")

# Serialize the combined graph to a new TTL file
g.serialize(destination="EnDe-Lite50.ttl", format="turtle")
