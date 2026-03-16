from rdflib import Graph, Namespace
from rdflib.namespace import RDF

INPUT_TTL = r"C:\Users\mazek.ZZIRKELL\reshacl_thesis\source\ShapesGraphs\DBpedia_SHACL_selected30.ttl"
OUTPUT_TTL = r"C:\Users\mazek.ZZIRKELL\reshacl_thesis\source\ShapesGraphs\DBpedia_SHACL_selected30_no_shin.ttl"

SH = Namespace("http://www.w3.org/ns/shacl#")

g = Graph()
g.parse(INPUT_TTL, format="turtle")

list_heads = []

# collect and remove sh:in triples
for s, _, head in list(g.triples((None, SH["in"], None))):
    list_heads.append(head)
    g.remove((s, SH["in"], head))

# remove RDF list nodes
for head in list_heads:
    current = head
    seen = set()

    while current and current != RDF.nil and current not in seen:
        seen.add(current)

        first_triples = list(g.triples((current, RDF.first, None)))
        rest_triples = list(g.triples((current, RDF.rest, None)))

        next_node = None
        if rest_triples:
            next_node = rest_triples[0][2]

        for t in first_triples:
            g.remove(t)
        for t in rest_triples:
            g.remove(t)

        # remove rdf:type rdf:List if present
        for t in list(g.triples((current, RDF.type, None))):
            g.remove(t)

        current = next_node

g.serialize(destination=OUTPUT_TTL, format="turtle")
print(f"Written cleaned file to: {OUTPUT_TTL}")