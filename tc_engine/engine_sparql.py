from __future__ import annotations

from typing import Iterable
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF
from rdflib.term import Identifier
from rdflib.plugins.sparql import prepareQuery


# ----------------------------
# Rewrite
# ----------------------------
def rewrite_shapes_target_classes_from_cache(
    shapes_graph: Graph,
    closure_cache: dict[URIRef, set[URIRef]]
) -> Graph:
    """
    For each shape that has sh:targetClass t, add sh:targetClass for every c in closure_cache[t].
    """
    SH = Namespace("http://www.w3.org/ns/shacl#")

    # Collect per-shape targets (only explicit sh:targetClass)
    shape_target_map: dict[Identifier, set[URIRef]] = {}
    for shape, _, cls in shapes_graph.triples((None, SH.targetClass, None)):
        if isinstance(cls, URIRef):
            shape_target_map.setdefault(shape, set()).add(cls)

    # Add closure classes per shape
    for shape, targets in shape_target_map.items():
        for t in targets:
            cc = closure_cache.get(t)
            if not cc:
                continue
            for c in cc:
                shapes_graph.add((shape, SH.targetClass, c))

    return shapes_graph


# ----------------------------
# SPARQL closure for all seeds in one query
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

def closure_cache_sparql_all(ont: Graph, seeds: set[URIRef]) -> dict[URIRef, set[URIRef]]:
    """
    Computes closure(seed) for every seed in one SPARQL query.
    Returns: { seed -> set(reachable) }
    """
    if not seeds:
        return {}
    values = _values_block_uris(seeds)
    q = prepareQuery(_QUERY_TEMPLATE % values)

    cache: dict[URIRef, set[URIRef]] = {s: {s} for s in seeds}
    for row in ont.query(q):
        seed = row.get("seed")
        c = row.get("c")
        if isinstance(seed, URIRef) and isinstance(c, URIRef):
            cache.setdefault(seed, set()).add(c)
    # Ensure every seed exists even if query returns nothing for it
    for s in seeds:
        cache.setdefault(s, {s})

    return cache



# ----------------------------
#expand + return updated shapes + expanded targets
# ----------------------------
def expand_target_classes_cached_sparql(
    shapes_graph: Graph,
    ontology_graph: Graph,
    seed_target_classes: set[URIRef]
) -> tuple[Graph, set[URIRef], dict[URIRef, set[URIRef]]]:
    """
    Uses SPARQL property paths to compute closures for all seed_target_classes in one query,
    then rewrites shapes_graph by adding sh:targetClass for closure members.
    """
    closure_cache = closure_cache_sparql_all(ontology_graph, seed_target_classes)

    expanded_global: set[URIRef] = set()
    for cc in closure_cache.values():
        expanded_global.update(cc)

    rewritten = rewrite_shapes_target_classes_from_cache(
        shapes_graph,
        closure_cache,
    )
    return rewritten, expanded_global, closure_cache



def main() -> None:
    ontology_path = "reshacl_thesis/source/datasets/dbpedia_ontology.owl"
    shapes_path = "reshacl_thesis/source/shapesg/Shape_30.ttl"

    ontology_graph = Graph()
    ontology_graph.parse(ontology_path, format="xml")

    shapes_graph = Graph()
    shapes_graph.parse(shapes_path, format="turtle")

    SH = Namespace("http://www.w3.org/ns/shacl#")
    seed_target_classes: set[URIRef] = {
        cls for _, _, cls in shapes_graph.triples((None, SH.targetClass, None))
        if isinstance(cls, URIRef)
    }

    rewritten, expanded_global, closure_cache = expand_target_classes_cached_sparql(
        shapes_graph=shapes_graph,
        ontology_graph=ontology_graph,
        seed_target_classes=seed_target_classes,
    )

    print(f"Seed targets: {len(seed_target_classes)}")
    print(f"Expanded targets (union of closures): {len(expanded_global)}")

    rewritten.serialize("Shape_30.expanded.ttl", format="turtle")
    print("Wrote: Shape_30.expanded.ttl")


if __name__ == "__main__":
    main()
