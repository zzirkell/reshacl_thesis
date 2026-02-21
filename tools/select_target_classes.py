#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract a SHACL subset from DBpedia_SHACL.ttl for a fixed set of target classes.

Key behavior:
- Select NodeShapes whose sh:targetClass is in TARGET_CLASSES (your chosen 30).
- Copy each selected shape + all reachable constraint nodes (blank nodes, RDF lists,
  and referenced sh:PropertyShape / sh:NodeShape definitions).
- IMPORTANT: Remove ALL sh:targetClass triples whose object is NOT in TARGET_CLASSES,
  so downstream scripts that scan for sh:targetClass will see exactly your 30 classes.

Usage:
  python extract_selected_30_dbpedia_shapes.py --in DBpedia_SHACL.ttl --out DBpedia_SHACL_selected30.ttl
"""

import argparse
from collections import deque
from rdflib import Graph, URIRef, BNode
from rdflib.namespace import RDF, RDFS, OWL, XSD, Namespace

SH = Namespace("http://www.w3.org/ns/shacl#")
RDFNS = RDF  # alias


# ---- Your chosen 30 target classes ----
TARGET_CLASSES = {
    URIRef("http://dbpedia.org/ontology/ReligiousBuilding"),
    URIRef("http://dbpedia.org/ontology/WrittenWork"),
    URIRef("http://dbpedia.org/ontology/AnatomicalStructure"),
    URIRef("http://dbpedia.org/ontology/Language"),
    URIRef("http://dbpedia.org/ontology/SportsEvent"),
    URIRef("http://dbpedia.org/ontology/Artwork"),
    URIRef("http://dbpedia.org/ontology/Award"),
    URIRef("http://dbpedia.org/ontology/PowerStation"),
    URIRef("http://dbpedia.org/ontology/Cleric"),
    URIRef("http://dbpedia.org/ontology/SportsTeam"),
    URIRef("http://dbpedia.org/ontology/Enzyme"),
    URIRef("http://dbpedia.org/ontology/Actor"),
    URIRef("http://dbpedia.org/ontology/Comic"),
    URIRef("http://dbpedia.org/ontology/AustralianRulesFootballPlayer"),
    URIRef("http://dbpedia.org/ontology/Swimmer"),
    URIRef("http://dbpedia.org/ontology/Museum"),
    URIRef("http://dbpedia.org/ontology/TennisPlayer"),
    URIRef("http://dbpedia.org/ontology/TennisTournament"),
    URIRef("http://dbpedia.org/ontology/Drug"),
    URIRef("http://dbpedia.org/ontology/Criminal"),
    URIRef("http://dbpedia.org/ontology/Lake"),
    URIRef("http://dbpedia.org/ontology/Weapon"),
    URIRef("http://dbpedia.org/ontology/EducationalInstitution"),
    URIRef("http://dbpedia.org/ontology/EthnicGroup"),
    URIRef("http://dbpedia.org/ontology/GovernmentAgency"),
    URIRef("http://dbpedia.org/ontology/Dam"),
    URIRef("http://dbpedia.org/ontology/Species"),
    URIRef("http://dbpedia.org/ontology/Place"),
    URIRef("http://dbpedia.org/ontology/Fish"),
    URIRef("http://dbpedia.org/ontology/Website"),
}


def parse_turtle(g: Graph, path: str) -> None:
    g.parse(path, format="turtle")


def is_named_shape(g: Graph, node: URIRef) -> bool:
    # Expand only if the node is explicitly a SHACL shape in the input graph
    return (
        (node, RDF.type, SH.NodeShape) in g
        or (node, RDF.type, SH.PropertyShape) in g
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Path to DBpedia_SHACL.ttl")
    ap.add_argument("--out", dest="out", required=True, help="Output TTL path")
    ap.add_argument("--keep-unselected-shapes", action="store_true",
                    help="If set: keep referenced NodeShapes even if they have non-selected targets (targets will still be stripped).")
    args = ap.parse_args()

    g = Graph()
    parse_turtle(g, args.inp)

    # 1) Find selected NodeShapes by targetClass
    selected_shapes = set()
    for s in g.subjects(RDF.type, SH.NodeShape):
        for tc in g.objects(s, SH.targetClass):
            if isinstance(tc, URIRef) and tc in TARGET_CLASSES:
                selected_shapes.add(s)
                break

    if not selected_shapes:
        raise SystemExit("No NodeShapes found with the selected target classes. Check input TTL / target list.")

    # 2) Traverse reachable constraint graph from those shapes
    out = Graph()
    for pfx, ns in g.namespaces():
        out.bind(pfx, ns)

    q = deque(selected_shapes)
    seen = set(selected_shapes)

    def enqueue(n):
        if n not in seen:
            seen.add(n)
            q.append(n)

    while q:
        s = q.popleft()

        # copy all outgoing triples
        for p, o in g.predicate_objects(s):
            out.add((s, p, o))

            # Always expand blank nodes (constraints, RDF lists, etc.)
            if isinstance(o, BNode):
                enqueue(o)

            # Expand named shapes only (PropertyShape/NodeShape definitions)
            elif isinstance(o, URIRef) and is_named_shape(g, o):
                # Optional: if it's a NodeShape with a targetClass outside our set,
                # you *can* still keep it as a referenced constraint shape; its targetClass
                # will be stripped later anyway. This helps keep validation semantics intact.
                if (o, RDF.type, SH.NodeShape) in g:
                    if args.keep_unselected_shapes:
                        enqueue(o)
                    else:
                        # If not keeping unselected shapes, only expand if it's one of our selected shapes.
                        if o in selected_shapes:
                            enqueue(o)
                else:
                    # PropertyShape
                    enqueue(o)

        # also copy rdf:first/rest if this node is a list node (blank-node lists are handled above,
        # but if some lists are named for some reason, this keeps them coherent)
        for p, o in g.predicate_objects(s):
            if p in (RDF.first, RDF.rest):
                out.add((s, p, o))
                if isinstance(o, BNode):
                    enqueue(o)

    # 3) CRITICAL: strip ALL targetClass triples not in your 30 classes
    removed = 0
    for s, tc in list(out.subject_objects(SH.targetClass)):
        if tc not in TARGET_CLASSES:
            out.remove((s, SH.targetClass, tc))
            removed += 1

    # 4) Optional: ensure selected shapes still have their selected targetClass triples
    # (they should, unless the original used non-URI targets)
    kept_targets = sorted({str(tc) for tc in out.objects(None, SH.targetClass)})
    kept_count = len(kept_targets)

    out.serialize(args.out, format="turtle")

    print("Input file:", args.inp)
    print("Output file:", args.out)
    print("Selected NodeShapes:", len(selected_shapes))
    print("Kept target classes after stripping:", kept_count)
    print("Removed non-selected sh:targetClass triples:", removed)
    print("Output triples:", len(out))

    if kept_count != 30:
        print("WARNING: kept target class count is not 30.")
        print("Kept targets:")
        for t in kept_targets:
            print("  ", t)


if __name__ == "__main__":
    main()
