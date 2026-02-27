from rdflib import Graph, URIRef, Literal, BNode

input_file = r"C:\Users\mazek.ZZIRKELL\reshacl_thesis\tools\output_dbp\dbp_closure_test_small.nt"
output_file = r"C:\Users\mazek.ZZIRKELL\reshacl_thesis\tools\output_dbp\smalltest.ttl"

g = Graph()

with open(input_file, "r", encoding="utf-8") as f:
    for i, line in enumerate(f, 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            g.parse(data=line, format="nt")
        except Exception as e:
            print(f"Skipped line {i}: {e}")

g.serialize(destination=output_file, format="turtle")
print("Conversion complete!")