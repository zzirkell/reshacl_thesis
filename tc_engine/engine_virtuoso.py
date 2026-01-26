import requests
from datetime import datetime, time
from pathlib import Path
#from engine_virtuoso_one import updated_shapes_graph

# --------------------
# Config
# --------------------
ENDPOINT = "http://localhost:8890/sparql"

G_ONTO   = "http://example.org/ontology_new"
G_SHAPES = "http://example.org/shapes"
G_WORK   = "http://example.org/shapes_work"

OUT_TTL  = "source/ShapesGraphs/expanded_shapes.ttl"

PREFIXES = """\
PREFIX sh:   <http://www.w3.org/ns/shacl#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""

# --------------------
# SPARQL helpers
# --------------------
def sparql_select(q: str, timeout: int = 60) -> dict:
    r = requests.post(
        ENDPOINT,
        data={"query": PREFIXES + q},
        headers={"Accept": "application/sparql-results+json"},
        timeout=timeout,
    )
    #r.raise_for_status()
    return r.json()

def sparql_update(u: str, timeout: int = 180) -> None:
    r = requests.post(
        ENDPOINT,
        data={"update": PREFIXES + u},
        headers={"Accept": "application/sparql-results+json"},
        timeout=timeout,
    )
    #r.raise_for_status()

def get_count(q: str, var: str) -> int:
    data = sparql_select(q)
    bindings = data["results"]["bindings"]
    if not bindings:
        return 0
    return int(bindings[0][var]["value"])

# --------------------
# clean ans copy shapes graph
# --------------------
def reset_work_graph() -> None:
    sparql_update(f"CLEAR GRAPH <{G_WORK}>;")

def copy_shapes_to_work() -> None:
    u = f"""
    INSERT {{
      GRAPH <{G_WORK}> {{ ?s ?p ?o }}
    }}
    WHERE {{
      GRAPH <{G_SHAPES}> {{ ?s ?p ?o }}
    }};
    """
    sparql_update(u, timeout=300)

# --------------------
# counts of what is discovered + update of the final graph
# --------------------
COUNT_EQ = f"""
SELECT (COUNT(*) AS ?missingEq)
WHERE {{
  GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?c . }}
  GRAPH <{G_ONTO}> {{
    {{ ?c owl:sameAs ?x . }}
    UNION {{ ?x owl:sameAs ?c . }}
    UNION {{ ?c owl:equivalentClass ?x . }}
    UNION {{ ?x owl:equivalentClass ?c . }}
  }}
  FILTER NOT EXISTS {{ GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?x }} }}
}}
"""

COUNT_SUB = f"""
SELECT (COUNT(*) AS ?missingSub)
WHERE {{
  GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?c . }}
  GRAPH <{G_ONTO}> {{ ?sub rdfs:subClassOf ?c . }}
  FILTER NOT EXISTS {{ GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?sub }} }}
}}
"""

UPDATE_EQ = f"""
INSERT {{
  GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?x . }}
}}
WHERE {{
  GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?c . }}
  GRAPH <{G_ONTO}> {{
    {{ ?c owl:sameAs ?x . }}
    UNION {{ ?x owl:sameAs ?c . }}
    UNION {{ ?c owl:equivalentClass ?x . }}
    UNION {{ ?x owl:equivalentClass ?c . }}
  }}
  FILTER NOT EXISTS {{ GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?x }} }}
}};
"""

UPDATE_SUB = f"""
INSERT {{
  GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?sub . }}
}}
WHERE {{
  GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?c . }}
  GRAPH <{G_ONTO}> {{ ?sub rdfs:subClassOf ?c . }}
  FILTER NOT EXISTS {{ GRAPH <{G_WORK}> {{ ?shape sh:targetClass ?sub }} }}
}};
"""

def expand_to_fixpoint(max_iters: int = 1000) -> None:
    total_est = 0
    for it in range(1, max_iters + 1):
        missing_eq = get_count(COUNT_EQ, "missingEq")
        missing_sub = get_count(COUNT_SUB, "missingSub")
        print(f"iter {it}: missingEq={missing_eq} missingSub={missing_sub}")

        if missing_eq == 0 and missing_sub == 0:
            print(f"Fixpoint reached after {it-1} iterations.")
            print(f"Estimated inserted pairs (upper bound): {total_est}")
            return

        if missing_eq:
            sparql_update(UPDATE_EQ)
        if missing_sub:
            sparql_update(UPDATE_SUB)

        total_est += (missing_eq + missing_sub)

    raise RuntimeError(f"Did not converge within {max_iters} iterations.")

# --------------------
# export the final graph
# --------------------
def export_work_graph_ttl(out_path: str = OUT_TTL, method: str = "default") -> Path:
    # if method == "updated_shapes_graph":
    #     r = updated_shapes_graph()
    #     p = Path(OUT_TTL)
    #     p.write_bytes(r.content)
    #     return p
    expand_to_fixpoint()
    construct = f"""
    CONSTRUCT {{
      ?s ?p ?o .
    }}
    WHERE {{
      GRAPH <{G_WORK}> {{
        ?s ?p ?o .
      }}
    }}
    """
    r = requests.post(
        ENDPOINT,
        data={"query": PREFIXES + construct},
        headers={"Accept": "text/turtle"},
        timeout=300,
    )
    p = Path(out_path)
    
    p.write_bytes(r.content)
    return p

# --------------------
# main
# --------------------
def main():
    

    # Create working graph
    reset_work_graph()
    copy_shapes_to_work()
    print("OK: shapes copied to shapes_work and still SHACL-safe")

    # Expand
    start = datetime.now()
    # Export
    p = export_work_graph_ttl(OUT_TTL)
    print("Wrote:", p.resolve(), "bytes:", p.stat().st_size)

    elapsed = datetime.now() - start
    print("Done. Elapsed:", elapsed)

if __name__ == "__main__":
    main()