"""
Compute key statistics for a SHACL shapes graph (.ttl).

- #Shapes (NodeShape / PropertyShape)
- #distinct target classes and targetClass triples
- #property shapes linked via sh:property (total + avg per NodeShape)
- #unique sh:path predicates (and top-N paths)
- count of shapes that constrain rdf:type path
- count of constraints using sh:in (and how often on rdf:type)
- basic triple count and term counts

Usage (Windows):
  python shapes_stats.py "C:\\Users\\mazek.ZZIRKELL\\reshacl_thesis\\source\\ShapesGraphs\\DBpedia_SHACL_selected30.ttl"
"""

from __future__ import annotations
import sys
from collections import Counter, defaultdict
from pathlib import Path

from rdflib import Graph, URIRef, BNode
from rdflib.namespace import RDF, XSD
from rdflib.namespace import Namespace

SH = Namespace("http://www.w3.org/ns/shacl#")
OWL = Namespace("http://www.w3.org/2002/07/owl#")


def _is_iri(x) -> bool:
    return isinstance(x, URIRef)


def _list_items(g: Graph, head) -> list:
    """Return python list for an RDF collection starting at head (rdf:first/rest)."""
    items = []
    while head and head != RDF.nil:
        first = g.value(head, RDF.first)
        if first is not None:
            items.append(first)
        head = g.value(head, RDF.rest)
    return items


def main(ttl_path: str, top_n_paths: int = 15) -> None:
    path = Path(ttl_path)
    if not path.exists():
        raise FileNotFoundError(path)

    g = Graph()
    g.parse(path.as_posix(), format="turtle")

    # --- basic graph stats ---
    triple_count = len(g)
    subj = set()
    pred = set()
    obj = set()
    iris = set()
    bnodes = set()
    literals = 0

    for s, p, o in g:
        subj.add(s); pred.add(p); obj.add(o)
        for term in (s, p, o):
            if isinstance(term, URIRef):
                iris.add(term)
            elif isinstance(term, BNode):
                bnodes.add(term)
        if not _is_iri(o) and not isinstance(o, BNode):
            literals += 1

    # --- shapes ---
    node_shapes = set(g.subjects(RDF.type, SH.NodeShape))
    prop_shapes = set(g.subjects(RDF.type, SH.PropertyShape))

    # any resource with sh:targetClass is effectively a "shape node" too
    shapes_with_target = set(g.subjects(SH.targetClass, None))
    # keep intersection with NodeShapes for "proper node shapes"
    node_shapes_with_target = node_shapes.intersection(shapes_with_target)

    # --- target classes ---
    target_classes = set(g.objects(None, SH.targetClass))
    target_class_triples = sum(1 for _ in g.triples((None, SH.targetClass, None)))

    # --- sh:property links: NodeShape -> PropertyShape ---
    ns_to_ps = defaultdict(set)
    for ns in node_shapes:
        for ps in g.objects(ns, SH.property):
            ns_to_ps[ns].add(ps)

    total_sh_property_links = sum(len(v) for v in ns_to_ps.values())
    avg_ps_per_nodeshape = (total_sh_property_links / len(node_shapes)) if node_shapes else 0.0

    # --- sh:path stats ---
    paths = []
    for ps in prop_shapes:
        for p in g.objects(ps, SH.path):
            paths.append(p)
    path_counter = Counter(paths)
    distinct_paths = len(path_counter)

    # rdf:type-constraining property shapes:
    # (a) sh:path rdf:type
    # (b) and some restriction (sh:in OR sh:class OR sh:datatype OR sh:nodeKind OR sh:hasValue)
    TYPE_PATH = RDF.type
    type_path_ps = set(s for s in prop_shapes if (s, SH.path, TYPE_PATH) in g)
    def _has_any_restriction(ps) -> bool:
        for pred_ in (SH["in"], SH["class"], SH.datatype, SH.nodeKind, SH.hasValue):
            if (ps, pred_, None) in g:
                return True
        return False

    type_path_ps_with_restr = set(ps for ps in type_path_ps if _has_any_restriction(ps))

    # sh:in usage
    in_count = sum(1 for _ in g.triples((None, SH["in"], None)))
    in_on_type_path = 0
    in_list_sizes = []
    for ps in prop_shapes:
        in_head = g.value(ps, SH["in"])
        if in_head is None:
            continue
        items = _list_items(g, in_head)
        in_list_sizes.append(len(items))
        if (ps, SH.path, TYPE_PATH) in g:
            in_on_type_path += 1

    # --- print report ---
    print("=== Shapes graph stats ===")
    print(f"File: {path}")
    print(f"Triples: {triple_count}")
    print(f"Distinct subjects/predicates/objects: {len(subj)} / {len(pred)} / {len(obj)}")
    print(f"Distinct IRIs: {len(iris)}")
    print(f"Distinct blank nodes: {len(bnodes)}")
    print(f"Literal objects: {literals}")
    print()
    print("=== Shapes ===")
    print(f"sh:NodeShape: {len(node_shapes)}")
    print(f"sh:PropertyShape: {len(prop_shapes)}")
    print(f"Shapes with sh:targetClass (any type): {len(shapes_with_target)}")
    print(f"NodeShapes with sh:targetClass: {len(node_shapes_with_target)}")
    print()
    print("=== Target classes ===")
    print(f"sh:targetClass triples: {target_class_triples}")
    print(f"Distinct target classes: {len(target_classes)}")
    print()
    print("=== Property-shape links ===")
    print(f"Total sh:property links (NodeShape -> PropertyShape): {total_sh_property_links}")
    print(f"Avg property shapes per NodeShape: {avg_ps_per_nodeshape:.2f}")
    print()
    print("=== sh:path ===")
    print(f"Distinct sh:path values: {distinct_paths}")
    print(f"Total PropertyShapes with sh:path: {len(paths)}")
    print(f"PropertyShapes with sh:path rdf:type: {len(type_path_ps)}")
    print(f"...with rdf:type and a restriction (in/class/datatype/nodeKind/hasValue): {len(type_path_ps_with_restr)}")
    print()
    print("Top sh:path values:")
    for p, c in path_counter.most_common(top_n_paths):
        print(f"  {p.n3(g.namespace_manager) if hasattr(p, 'n3') else str(p)} : {c}")
    print()
    print("=== sh:in usage ===")
    print(f"Total sh:in occurrences: {in_count}")
    print(f"sh:in occurrences on rdf:type path: {in_on_type_path}")
    if in_list_sizes:
        in_list_sizes_sorted = sorted(in_list_sizes)
        avg_in = sum(in_list_sizes_sorted) / len(in_list_sizes_sorted)
        median_in = in_list_sizes_sorted[len(in_list_sizes_sorted)//2]
        print(f"sh:in list size (avg / median / min / max): "
              f"{avg_in:.2f} / {median_in} / {min(in_list_sizes_sorted)} / {max(in_list_sizes_sorted)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python shapes_stats.py <path_to_shapes_ttl>")
        sys.exit(1)
    main(sys.argv[1])