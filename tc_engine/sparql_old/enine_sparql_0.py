from __future__ import annotations

import time
import requests
from typing import Iterable, Dict, Set, List
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF
from rdflib.term import Identifier

SH = Namespace("http://www.w3.org/ns/shacl#")


# ----------------------------
# Rewrite
# ----------------------------
def rewrite_shapes_target_classes_from_cache(
    shapes_graph: Graph,
    closure_cache: Dict[URIRef, Set[URIRef]]
) -> Graph:
    """
    For each shape that has sh:targetClass t, add sh:targetClass for every c in closure_cache[t].
    """
    shape_target_map: Dict[Identifier, Set[URIRef]] = {}
    for shape, _, cls in shapes_graph.triples((None, SH.targetClass, None)):
        if isinstance(cls, URIRef):
            shape_target_map.setdefault(shape, set()).add(cls)

    for shape, targets in shape_target_map.items():
        for t in targets:
            cc = closure_cache.get(t)
            if not cc:
                continue
            for c in cc:
                shapes_graph.add((shape, SH.targetClass, c))

    return shapes_graph


# ----------------------------
# SPARQL closure query (bulk)
# ----------------------------
_QUERY_TEMPLATE = """
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?seed ?c WHERE {
  VALUES ?seed { %s }

  ?seed ( owl:equivalentClass | ^owl:equivalentClass
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


def closure_cache_sparql_all_graphdb(
    seeds: Set[URIRef],
    *,
    endpoint_url: str,
    username: str,
    password: str,
    chunk_size: int = 40,
    timeout_s: int = 120,
    max_retries: int = 3,
    sleep_s: float = 1.0,
) -> Dict[URIRef, Set[URIRef]]:
    """
    Compute closure(seed) for all seeds by calling DBpedia's public Virtuoso endpoint.
    Uses chunking + retries because public endpoints may timeout or rate-limit.

    Returns: { seed -> set(reachable classes) }
    """
    if not seeds:
        return {}

    headers = {
        # being explicit helps public endpoints
        "Accept": "application/sparql-results+json",
        "User-Agent": "reshacl-thesis-targetclass-rewriter/1.0",
    }

    seed_list = list(seeds)
    cache: Dict[URIRef, Set[URIRef]] = {s: {s} for s in seed_list}

    for chunk in _chunk_list(seed_list, chunk_size):
        query = _QUERY_TEMPLATE % _values_block_uris(chunk)

        # retry loop per chunk
        for attempt in range(max_retries):
            try:
                r = requests.post(
                    endpoint_url,
                    data={"query": query},
                    headers={**headers, "Content-Type": "application/x-www-form-urlencoded"},
                    auth=(username, password),
                    timeout=timeout_s,
                )

                #r = requests.get(endpoint_url, params=params, headers=headers, timeout=timeout_s)
                r.raise_for_status()
                data = r.json()

                for b in data.get("results", {}).get("bindings", []):
                    seed_val = b.get("seed", {}).get("value")
                    c_val = b.get("c", {}).get("value")
                    if seed_val and c_val:
                        s_iri = URIRef(seed_val)
                        c_iri = URIRef(c_val)
                        cache.setdefault(s_iri, set()).add(c_iri)

                break  # success

            except Exception as e:
                # exponential-ish backoff
                if attempt == max_retries - 1:
                    raise RuntimeError(f"DBpedia endpoint failed for chunk (size={len(chunk)}): {e}") from e
                time.sleep(sleep_s * (2 ** attempt))

        # be nice to the public endpoint
        time.sleep(sleep_s)

    # Ensure every seed exists
    for s in seed_list:
        cache.setdefault(s, {s})

    return cache


# ----------------------------
# expand + rewrite shapes
# ----------------------------
def expand_target_classes_cached_sparql_dbpedia(
    shapes_graph: Graph,
    seed_target_classes: Set[URIRef]
) -> tuple[Graph, Set[URIRef], Dict[URIRef, Set[URIRef]]]:
    """
    Query DBpedia endpoint for closures, then rewrite shapes_graph by adding sh:targetClass for closure members.
    """
    closure_cache = closure_cache_sparql_all_graphdb(
                        seed_target_classes,
                        endpoint_url="https://vd36cfa116f4f4878832.sandbox.graphwise.ai/repositories/thesis",
                        username="mazekaras@gmail.com",
                        password="gr#ehw9{!W",
                    )

    expanded_global: Set[URIRef] = set()
    for cc in closure_cache.values():
        expanded_global.update(cc)

    rewritten = rewrite_shapes_target_classes_from_cache(shapes_graph, closure_cache)
    return rewritten, expanded_global, closure_cache


def main() -> None:
    shapes_path = "reshacl_thesis/source/ShapesGraphs/Shape_30.ttl"

    shapes_graph = Graph()
    shapes_graph.parse(shapes_path, format="turtle")

    seed_target_classes: Set[URIRef] = {
        cls for _, _, cls in shapes_graph.triples((None, SH.targetClass, None))
        if isinstance(cls, URIRef)
    }

    rewritten, expanded_global, closure_cache = expand_target_classes_cached_sparql_dbpedia(
        shapes_graph=shapes_graph,
        seed_target_classes=seed_target_classes,
    )

    print(f"Seed targets: {len(seed_target_classes)}")
    print(f"Expanded targets (union of closures): {len(expanded_global)}")

    rewritten.serialize("Shape_30.expanded.dbpedia.ttl", format="turtle")
    print("Wrote: Shape_30.expanded.dbpedia.ttl")


if __name__ == "__main__":
    main()
