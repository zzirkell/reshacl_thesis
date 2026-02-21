import sys
from rdflib import Graph

nt_in = sys.argv[1]
ttl_out = sys.argv[2]

g = Graph()
g.parse(nt_in, format="nt")
g.serialize(ttl_out, format="turtle")

print("Wrote", ttl_out, "triples:", len(g))
