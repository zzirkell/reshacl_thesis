from __future__ import annotations

from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import OWL, RDFS, RDF
from rdflib.term import Identifier
import subprocess
from pathlib import Path


def class_closure(ont: Graph, start: URIRef) -> set[URIRef]:
    """
    Deep closure over a class URIRef via:
      - owl:equivalentClass (both directions, any depth)
      - owl:sameAs (both directions, any depth)
      - rdfs:subClassOf (downwards: subclasses, any depth)

    Returns a set that always includes `start`.
    """
    visited: set[URIRef] = {start}
    stack: list[URIRef] = [start]

    while stack:
        x = stack.pop()
        # eq/sameAs (both directions)
        for p in (OWL.equivalentClass, OWL.sameAs):
            for y in ont.objects(x, p):
                if isinstance(y, URIRef) and y not in visited:
                    visited.add(y)
                    stack.append(y)
            for y in ont.subjects(p, x):
                if isinstance(y, URIRef) and y not in visited:
                    visited.add(y)
                    stack.append(y)
        # subclasses (downwards)
        for y in ont.subjects(RDFS.subClassOf, x):
            if isinstance(y, URIRef) and y not in visited:
                visited.add(y)
                stack.append(y)
    return visited


def rewrite_shapes_target_classes_from_cache(
    shapes_graph: Graph,
    closure_cache: dict[URIRef, set[URIRef]],
) -> Graph:
    """
    For each shape that has sh:targetClass t, add sh:targetClass for every c in closure_cache[t].
    """
    SH = Namespace("http://www.w3.org/ns/shacl#")

    #Collect per-shape targets (only explicit sh:targetClass)
    shape_target_map: dict[Identifier, set[URIRef]] = {}
    for shape, _, cls in shapes_graph.triples((None, SH.targetClass, None)):
        if isinstance(cls, URIRef):
            shape_target_map.setdefault(shape, set()).add(cls)

    #Add closure classes per shape
    for shape, targets in shape_target_map.items():
        for t in targets:
            cc = closure_cache.get(t)
            if not cc:
                continue
            for c in cc:
                shapes_graph.add((shape, SH.targetClass, c))
    return shapes_graph

def expand_target_classes_cached(
    shapes_graph: Graph,
    ontology_graph: Graph,
    seed_target_classes: set[URIRef],
) -> tuple[Graph, set[URIRef], dict[URIRef, set[URIRef]]]:
    SH = Namespace("http://www.w3.org/ns/shacl#")
    # Build cache
    closure_cache: dict[URIRef, set[URIRef]] = {}
    expanded_global: set[URIRef] = set()

    for c in seed_target_classes:
        if isinstance(c, URIRef) and c not in closure_cache:
            cc = class_closure(ontology_graph, c)
            closure_cache[c] = cc
            expanded_global.update(cc)

    #Rewrite shapes using the cache
    rewritten = rewrite_shapes_target_classes_from_cache(
        shapes_graph,
        closure_cache,
    )

    return rewritten, expanded_global, closure_cache

def closure_to_dot(
    ontology_graph: Graph,
    closure_cache: dict[URIRef, set[URIRef]],
) -> str:
    """
    Build a Graphviz DOT graph for all target class closures.
    """
    def node_id(u: URIRef) -> str:
        return "n_" + str(abs(hash(u)))

    def label(u: URIRef) -> str:
        return ontology_graph.namespace_manager.normalizeUri(u)

    # union of all closure nodes
    nodes: set[URIRef] = set()
    for cc in closure_cache.values():
        nodes.update(cc)

    seed_targets = set(closure_cache.keys())

    lines = []
    lines.append("digraph ClassClosure {")
    lines.append("  rankdir=LR;")
    lines.append("  node [shape=box, fontsize=10];")

    # nodes
    for u in nodes:
        attrs = []
        if u in seed_targets:
            attrs.append("penwidth=2")
        lines.append(
            f'  {node_id(u)} [label="{label(u)}"{", " if attrs else ""}{" ".join(attrs)}];'
        )

    # edges: rdfs:subClassOf
    for sub, _, sup in ontology_graph.triples((None, RDFS.subClassOf, None)):
        if sub in nodes and sup in nodes:
            lines.append(
                f'  {node_id(sub)} -> {node_id(sup)} [label="subClassOf"];'
            )

    # edges: equivalentClass + sameAs (both directions)
    for p, lab in ((OWL.equivalentClass, "equivalentClass"), (OWL.sameAs, "sameAs")):
        for a, _, b in ontology_graph.triples((None, p, None)):
            if a in nodes and b in nodes:
                lines.append(
                    f'  {node_id(a)} -> {node_id(b)} [label="{lab}"];'
                )
                lines.append(
                    f'  {node_id(b)} -> {node_id(a)} [label="{lab}"];'
                )

    lines.append("}")
    return "\n".join(lines)


#here you can output the png of the shape expanded with new target classes
def main() -> None:
    ontology_path = "reshacl_thesis/source/datasets/dbpedia_ontology.owl"
    shapes_path = "reshacl_thesis/source/shapesg/Shape_30.ttl"

    ontology_graph = Graph()
    ontology_graph.parse(ontology_path, format="xml")

    shapes_graph = Graph()
    shapes_graph.parse(shapes_path, format="turtle")

    # single pass: get seed target classes (you said you already have them; this is the minimal way)
    SH = Namespace("http://www.w3.org/ns/shacl#")
    seed_target_classes: set[URIRef] = {
        cls for _, _, cls in shapes_graph.triples((None, SH.targetClass, None))
        if isinstance(cls, URIRef)
    }

    _, _, closure_cache = expand_target_classes_cached(
        shapes_graph=shapes_graph,
        ontology_graph=ontology_graph,
        seed_target_classes=seed_target_classes,
    )

    dot = closure_to_dot(ontology_graph, closure_cache)

    dot_path = Path("target_class_closure.dot")
    png_path = Path("target_class_closure.png")
    dot_path.write_text(dot, encoding="utf-8")

    subprocess.run(["dot", "-Tpng", str(dot_path), "-o", str(png_path)], check=True)

    print(f"✔ wrote {dot_path}")
    print(f"✔ wrote {png_path}")


if __name__ == "__main__":
    main()
