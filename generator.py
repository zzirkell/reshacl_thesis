from __future__ import annotations

import os
import random
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL, XSD

EX = Namespace("http://example.org/")
SH = Namespace("http://www.w3.org/ns/shacl#")


@dataclass
class ComponentSpec:
    name: str                 # e.g., "A" or "D"
    depth: int                # hierarchy depth
    branching: int            # children per node
    n_instances_per_leaf: int # how many individuals per leaf


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def class_iri(comp: str, idx: int) -> URIRef:
    return EX[f"{comp}{idx}"]


def inst_iri(comp: str, leaf_idx: int, inst_idx: int) -> URIRef:
    return EX[f"i_{comp}{leaf_idx}_{inst_idx}"]


def build_tree_nodes(depth: int, branching: int) -> Tuple[List[int], Dict[int, List[int]], Dict[int, int]]:
    """
    Build a synthetic rooted tree using integer IDs for nodes.
    Root is 0. Children numbered consecutively.
    Returns: (all_nodes, children_map, parent_map)
    """
    children: Dict[int, List[int]] = {}
    parent: Dict[int, int] = {}
    all_nodes: List[int] = [0]
    frontier: List[Tuple[int, int]] = [(0, 0)]  # (node, level)
    next_id = 1

    while frontier:
        n, lvl = frontier.pop(0)
        if lvl >= depth:
            continue
        kids = []
        for _ in range(branching):
            k = next_id
            next_id += 1
            kids.append(k)
            parent[k] = n
            all_nodes.append(k)
            frontier.append((k, lvl + 1))
        children[n] = kids

    return all_nodes, children, parent


def leaf_nodes(all_nodes: List[int], children: Dict[int, List[int]]) -> List[int]:
    kids_set = set(children.keys())
    return [n for n in all_nodes if n not in kids_set]


def generate_component_ontology(g: Graph, spec: ComponentSpec) -> Tuple[URIRef, List[URIRef], Dict[URIRef, URIRef]]:
    """
    Generate class axioms for one component:
      - a pure rdfs:subClassOf tree
      - plus a few owl:equivalentClass and owl:sameAs links (between some nodes)
    Returns:
      root_class, leaf_classes, parent_of (class -> parent class)
    """
    all_ids, children_map, parent_map = build_tree_nodes(spec.depth, spec.branching)
    id_to_class: Dict[int, URIRef] = {i: class_iri(spec.name, i) for i in all_ids}

    # declare classes + subclass edges
    parent_of: Dict[URIRef, URIRef] = {}
    for i in all_ids:
        c = id_to_class[i]
        g.add((c, RDF.type, OWL.Class))
        if i != 0:
            p = id_to_class[parent_map[i]]
            g.add((c, RDFS.subClassOf, p))
            parent_of[c] = p

    root = id_to_class[0]
    leaves = [id_to_class[i] for i in leaf_nodes(all_ids, children_map)]

    # add a few equivalence links to create "non-binary tree" feel
    # (equiv/sameAs edges create extra branches during closure)
    rng = random.Random(7 + hash(spec.name) % 10000)
    candidates = [id_to_class[i] for i in all_ids if i != 0]
    rng.shuffle(candidates)

    # add 2 equivalentClass links (bidirectional semantics)
    for k in range(min(2, len(candidates) // 2)):
        a = candidates[2*k]
        b = candidates[2*k + 1]
        g.add((a, OWL.equivalentClass, b))

    # add 1 sameAs link (also treated as equivalence in your thesis scope)
    if len(candidates) >= 4:
        g.add((candidates[3], OWL.sameAs, candidates[4]))

    return root, leaves, parent_of


def generate_data_without_supertype_injection(
    g: Graph,
    spec: ComponentSpec,
    leaf_classes: List[URIRef],
) -> None:
    """
    Data graph:
      - Individuals are typed ONLY to leaf classes (no supertype rdf:type!)
      - This is the key: we avoid any inferred type injection.
    """
    for leaf in leaf_classes:
        # leaf index parsed from IRI local name (e.g., A13 -> 13)
        local = str(leaf).split("/")[-1]
        leaf_idx = int("".join(ch for ch in local if ch.isdigit()) or "0")

        for i in range(spec.n_instances_per_leaf):
            inst = inst_iri(spec.name, leaf_idx, i)
            g.add((inst, RDF.type, leaf))  # ONLY leaf type, nothing else


def generate_shapes(g: Graph, roots: List[URIRef]) -> None:
    """
    Shapes graph: 1 NodeShape per root, targeting only the root class.
    (Your rewrite should expand this with subclass/eq/sameAs closure.)
    """
    for idx, root in enumerate(roots, start=1):
        s = EX[f"S{idx}"]
        g.add((s, RDF.type, SH.NodeShape))
        g.add((s, SH.targetClass, root))


def main() -> None:
    out_dir = os.path.join("Outputs", "SynthGen")
    ensure_dir(out_dir)

    # --- Tweak these to scale the stress test ---
    # Two disconnected components:
    #   Component A: big-ish
    #   Component D: separate (so you can see disconnected closure)
    specs = [
        ComponentSpec(name="A", depth=4, branching=3, n_instances_per_leaf=200),  # many leaves
        ComponentSpec(name="D", depth=3, branching=3, n_instances_per_leaf=150),
    ]

    ont = Graph()
    data = Graph()
    shapes = Graph()

    ont.bind("ex", EX)
    data.bind("ex", EX)
    shapes.bind("ex", EX)
    shapes.bind("sh", SH)

    roots: List[URIRef] = []

    for spec in specs:
        root, leaves, _ = generate_component_ontology(ont, spec)
        roots.append(root)
        generate_data_without_supertype_injection(data, spec, leaves)

    generate_shapes(shapes, roots)

    # Serialize
    ont_path = os.path.join(out_dir, "ontology.ttl")
    data_path = os.path.join(out_dir, "data.ttl")
    shapes_path = os.path.join(out_dir, "shapes.ttl")

    ont.serialize(ont_path, format="turtle")
    data.serialize(data_path, format="turtle")
    shapes.serialize(shapes_path, format="turtle")

    print("Wrote:")
    print(" ", ont_path, f"({len(ont)} triples)")
    print(" ", data_path, f"({len(data)} triples)")
    print(" ", shapes_path, f"({len(shapes)} triples)")
    print("\nNote: data has rdf:type ONLY for leaves. No superclasses/types injected.")


if __name__ == "__main__":
    main()
