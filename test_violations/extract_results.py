from rdflib import Graph

QUERY = """
PREFIX sh: <http://www.w3.org/ns/shacl#>

SELECT DISTINCT
  ?focus
  ?shape
  ?path
  ?component
  ?value
WHERE {
  ?r a sh:ValidationResult ;
     sh:focusNode ?focus ;
     sh:sourceShape ?shape ;
     sh:sourceConstraintComponent ?component .

  OPTIONAL { ?r sh:resultPath ?path }
  OPTIONAL { ?r sh:value ?value }
}
ORDER BY ?focus ?shape ?path ?component ?value
"""

def extract(ttl_path: str, out_csv: str):
    g = Graph()
    g.parse(ttl_path, format="turtle")

    rows = []
    for row in g.query(QUERY):
        # row can contain None for OPTIONAL vars
        focus, shape, path, comp, value = row
        rows.append((
            str(focus),
            str(shape),
            "" if path is None else str(path),
            str(comp),
            "" if value is None else str(value),
        ))

    with open(out_csv, "w", encoding="utf-8") as f:
        f.write("focus,shape,path,component,value\n")
        for r in rows:
            f.write(",".join(x.replace(",", "%2C") for x in r) + "\n")

    print(f"Wrote {len(rows)} rows -> {out_csv}")

if __name__ == "__main__":
    # change these two paths
    extract("Outputs/EnDe-Lite50/violationGraph/ReSHACL_results.ttl", "original.csv")
    extract("Outputs/EnDe-Lite50/violationGraph/ReSHACL+Engine-RDFlib_results.ttl", "engine.csv")
