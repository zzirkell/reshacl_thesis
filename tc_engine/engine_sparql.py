from __future__ import annotations

import time
import requests
from typing import Dict, Iterable, List, Set, Tuple

from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF
from rdflib.term import Identifier

# Namespaces
SH = Namespace("http://www.w3.org/ns/shacl#")

# Virtuoso SPARQL endpoint + graph where your ontology lives
VIRTUOSO_ENDPOINT = "http://localhost:8890/sparql"
ONTO_GRAPH = "http://localhost:8890/DAV"   # <-- from your "list graphs" query


# ----------------------------
# SPARQL: closure query
# ----------------------------
# Semantics we want:
#  - start at seed
#  - move any number of times via:
#       owl:equivalentClass, inverse equivalentClass
#       owl:sameAs, inverse sameAs
#  - then go down to subclasses (any depth): ^rdfs:subClassOf*
#  - for each reached subclass, also include its equivalence closure (any depth)
#
# This matches your DFS-style expansion: eq/sameAs closure at every level + subclasses.
_QUERY_TEMPLATE = """
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?seed ?c WHERE {
  VALUES ?seed0 { %s }

  BIND(?seed0 AS ?seed)

  ?seed
    ( owl:equivalentClass | ^owl:equivalentClass
    | owl:sameAs         | ^owl:sameAs
    | ^rdfs:subClassOf
    )* ?c .

  FILTER(isIRI(?c))
}
"""


def _values_block_uris(seeds: Iterable[URIRef]) -> str:
    return " ".join(f"<{str(s)}>" for s in seeds if isinstance(s, URIRef))


def _chunk_list(xs: List[URIRef], n: int) -> Iterable[List[URIRef]]:
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


# ----------------------------
# Virtuoso query helper (fast + reliable)
# ----------------------------
def virtuoso_closure_cache(
    seeds: Set[URIRef],
    *,
    endpoint_url: str = VIRTUOSO_ENDPOINT,
    graph_iri: str = ONTO_GRAPH,
    chunk_size: int = 60,
    timeout_s: int = 60,
    max_retries: int = 3,
) -> Dict[URIRef, Set[URIRef]]:
    """
    Compute closure(seed) for all seeds using Virtuoso SPARQL (chunked).
    Returns: { seed -> set(closure classes) }
    """
    if not seeds:
        return {}

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "reshacl-thesis-tc-engine/1.0",
    }

    seed_list = list(seeds)
    cache: Dict[URIRef, Set[URIRef]] = {s: {s} for s in seed_list}

    for chunk in _chunk_list(seed_list, chunk_size):
        q = _QUERY_TEMPLATE % _values_block_uris(chunk)

        # Virtuoso: put ontology graph into the query scope via default-graph-uri
        params = {"default-graph-uri": graph_iri}
        data = {"query": q}

        last_err = None
        for attempt in range(max_retries):
            try:
                r = requests.post(
                    endpoint_url,
                    params=params,
                    data=data,
                    headers=headers,
                    timeout=timeout_s,
                )

                if r.status_code >= 400:
                    raise RuntimeError(
                        f"Virtuoso HTTP {r.status_code}\n"
                        f"URL: {r.url}\n"
                        f"Response (first 2000 chars):\n{r.text[:20000]}"
                    )
                r.raise_for_status()
                js = r.json()

                for b in js.get("results", {}).get("bindings", []):
                    seed_val = b.get("seed", {}).get("value")
                    c_val = b.get("c", {}).get("value")
                    if seed_val and c_val:
                        s_iri = URIRef(seed_val)
                        c_iri = URIRef(c_val)
                        cache.setdefault(s_iri, set()).add(c_iri)

                last_err = None
                break
            except Exception as e:
                last_err = e
                # short backoff (local endpoint usually doesn't need much)
                time.sleep(0.25 * (2 ** attempt))

        if last_err is not None:
            raise RuntimeError(f"Virtuoso query failed after retries: {last_err}") from last_err

    # Ensure every seed exists
    for s in seed_list:
        cache.setdefault(s, {s})

    return cache


# ----------------------------
# Shapes rewriting
# ----------------------------
def rewrite_shapes_target_classes_from_cache(
    shapes_graph: Graph,
    closure_cache: Dict[URIRef, Set[URIRef]],
    *,
    mark_node_equivalent: bool = True,
) -> Tuple[Graph, Set[URIRef], Dict[Identifier, Set[URIRef]]]:
    """
    For each shape with sh:targetClass t:
      - add sh:targetClass for every class in closure_cache[t]
      - optionally add rdf:type sh:NodeEquivalent
    Returns:
      rewritten shapes graph,
      expanded_global (union of all closure classes),
      shape_target_map (original explicit target classes per shape)
    """
    shape_target_map: Dict[Identifier, Set[URIRef]] = {}

    # collect explicit target classes
    for shape, _, cls in shapes_graph.triples((None, SH.targetClass, None)):
        if isinstance(cls, URIRef):
            shape_target_map.setdefault(shape, set()).add(cls)

    expanded_global: Set[URIRef] = set()

    # rewrite
    for shape, targets in shape_target_map.items():
        closure_union: Set[URIRef] = set()
        for t in targets:
            closure_union.update(closure_cache.get(t, {t}))

        expanded_global.update(closure_union)

        for c in closure_union:
            shapes_graph.add((shape, SH.targetClass, c))

        if mark_node_equivalent:
            shapes_graph.add((shape, RDF.type, SH.NodeEquivalent))

    return shapes_graph, expanded_global, shape_target_map


# ----------------------------
# Main entrypoint
# ----------------------------
def expand_target_classes_and_rewrite_shapes(
    shapes_path: Graph,
    seeds: set[URIRef],
    *,
    out_path: str = "Shape_30.expanded.virtuoso.ttl",
) -> Tuple[Set[URIRef], Graph]:
    """
    Load shapes TTL, compute closures via Virtuoso, rewrite shapes, and serialize.
    Returns: (expanded_target_classes_union, rewritten_shapes_graph)
    """
    

    if not seeds:
        raise RuntimeError("No sh:targetClass found in the shapes graph.")

    closure_cache = virtuoso_closure_cache(seeds)

    rewritten, expanded_global, shape_target_map = rewrite_shapes_target_classes_from_cache(
        shapes_path,
        closure_cache,
        mark_node_equivalent=True,
    )

    rewritten.serialize(out_path, format="turtle")

    # print small summary
    # print(f"Virtuoso endpoint: {VIRTUOSO_ENDPOINT}")
    # print(f"Ontology graph:    {ONTO_GRAPH}")
    # print(f"Shapes:            {shapes_path}")
    # print(f"Output:            {out_path}")
    # print(f"Seeds:             {len(seeds)}")
    # print(f"Expanded (union):  {len(expanded_global)}")

    return rewritten, expanded_global


# if __name__ == "__main__":
#     # change this to your file
#     expand_target_classes_and_rewrite_shapes(
#         shapes_path="source/ShapesGraphs/Shape_30.ttl",
#         out_path="Shape_30.expanded.virtuoso.ttl",
#     )