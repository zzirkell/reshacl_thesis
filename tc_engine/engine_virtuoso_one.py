import requests
from pathlib import Path

ENDPOINT = "http://localhost:8890/sparql"

G_ONTO   = "http://example.org/ontology"
G_SHAPES = "http://example.org/shapes"
G_WORK   = "http://example.org/shapes_work"

OUT_TTL  = "source/ShapesGraphs/expanded_shapes.ttl"

PREFIXES = """\
PREFIX sh:   <http://www.w3.org/ns/shacl#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""

QUERY = """
PREFIX sh:   <http://www.w3.org/ns/shacl#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
  ?s ?p ?o .
  ?shape sh:targetClass ?t .
}
WHERE {
  {
    GRAPH <http://example.org/shapes> {
      ?s ?p ?o .
    }
  }
  UNION
  {
    GRAPH <http://example.org/shapes> {
      ?shape sh:targetClass ?c0 .
    }
    GRAPH <http://example.org/ontology> {
      ?c0 (owl:sameAs|^owl:sameAs|owl:equivalentClass|^owl:equivalentClass|^rdfs:subClassOf)* ?t .
    }
  }
}
"""
def updated_shapes_graph():
    r = requests.post(
        ENDPOINT,
        data={"query": QUERY},
        headers={"Accept": "text/turtle"},
        timeout=180,
    )
    if r.status_code >= 400:
        print("HTTP", r.status_code)
        print(r.text[:4000])  # show Virtuoso error details
        r.raise_for_status()
    return r

def main():
    r = requests.post(
        ENDPOINT,
        data={"query": QUERY},
        headers={"Accept": "text/turtle"},
        timeout=180,
    )
    if r.status_code >= 400:
        print("HTTP", r.status_code)
        print(r.text[:4000])  # show Virtuoso error details
        r.raise_for_status()

    out = Path("expanded_shapes.ttl")
    out.write_bytes(r.content)
    print("Wrote", out.resolve())

if __name__ == "__main__":
    main()