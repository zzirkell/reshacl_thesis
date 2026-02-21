#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rank DBpedia SHACL NodeShapes by ontology-closure richness (offline).

Inputs:
  - shapes TTL (e.g., DBpedia_SHACL.ttl) that contains NodeShapes + void:entities
  - ontology OWL (local) to compute closure over:
      rdfs:subClassOf (downward)
      owl:equivalentClass, owl:sameAs (undirected), between named classes

Outputs:
  - CSV ranking
  - TTL subset graph containing selected shapes (+ referenced property/constraint nodes)

Usage example:
  python rank_dbpedia_shapes.py \
    --shapes DBpedia_SHACL.ttl \
    --ontology dbpedia_ontology.owl \
    --top-k 30 \
    --inst-transform sqrt \
    --max-entities 50000 \
    --max-closure 250 \
    --max-props 80 \
    --subset-out DBpedia_SHACL_top30.ttl \
    --csv-out ranking.csv
"""

import argparse
import csv
import math
import os
import pickle
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional, Set, Tuple

from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import RDF, RDFS, OWL, Namespace

SH = Namespace("http://www.w3.org/ns/shacl#")
VOID = Namespace("http://rdfs.org/ns/void#")


@dataclass
class ShapeStats:
    shape: str
    target_classes: str              # ';'-joined
    entities: Optional[int]
    prop_shapes: int
    total_sub: int
    total_eq: int
    closure_size: int
    total_layers: int
    score: float
    score_rich_linear: float         # for comparison/debug


def parse_any(graph: Graph, path: str) -> None:
    """Try multiple RDF formats for robust parsing."""
    # rdflib can often guess, but .owl may need explicit xml
    tried = []
    for fmt in (None, "xml", "turtle", "nt", "n3", "trig"):
        try:
            graph.parse(path, format=fmt)
            return
        except Exception as e:
            tried.append((fmt, str(e)[:200]))
    msg = "Failed to parse RDF file. Tried formats: " + ", ".join(str(t[0]) for t in tried)
    raise RuntimeError(msg)


def build_ontology_index(
    ont_path: str,
    cache_path: Optional[str] = None,
    class_prefixes: Optional[List[str]] = None,
) -> Tuple[Dict[URIRef, Set[URIRef]], Dict[URIRef, Set[URIRef]]]:
    """
    Build adjacency:
      sub_down[super] -> {subclasses}
      eq_adj[c] -> {equiv/sameAs neighbors}
    """
    if cache_path and os.path.exists(cache_path):
        with open(cache_path, "rb") as f:
            return pickle.load(f)

    g = Graph()
    parse_any(g, ont_path)

    def keep(u: URIRef) -> bool:
        if class_prefixes is None:
            return True
        s = str(u)
        return any(s.startswith(p) for p in class_prefixes)

    sub_down: Dict[URIRef, Set[URIRef]] = defaultdict(set)
    eq_adj: Dict[URIRef, Set[URIRef]] = defaultdict(set)

    # rdfs:subClassOf edges (sub -> super) => index downward from super
    for sub, _, sup in g.triples((None, RDFS.subClassOf, None)):
        if isinstance(sub, URIRef) and isinstance(sup, URIRef) and keep(sub) and keep(sup):
            sub_down[sup].add(sub)

    # owl:equivalentClass
    for a, _, b in g.triples((None, OWL.equivalentClass, None)):
        if isinstance(a, URIRef) and isinstance(b, URIRef) and keep(a) and keep(b):
            eq_adj[a].add(b)
            eq_adj[b].add(a)

    # owl:sameAs (only treat as class-level when both are in class prefixes)
    for a, _, b in g.triples((None, OWL.sameAs, None)):
        if isinstance(a, URIRef) and isinstance(b, URIRef) and keep(a) and keep(b):
            eq_adj[a].add(b)
            eq_adj[b].add(a)

    if cache_path:
        with open(cache_path, "wb") as f:
            pickle.dump((sub_down, eq_adj), f)

    return sub_down, eq_adj


def closure_fixpoint(
    seeds: Set[URIRef],
    sub_down: Dict[URIRef, Set[URIRef]],
    eq_adj: Dict[URIRef, Set[URIRef]],
    max_closure: int,
    max_layers: int,
) -> Tuple[Set[URIRef], int, int, int]:
    """
    Closure using rounds (fixpoint iterations):
      In each round, for every class currently in closure:
        - add eq neighbors (equivalentClass/sameAs)
        - add subclasses (downward subClassOf)

    Returns:
      closure_set, total_sub_added, total_eq_added, layers
    """
    closure: Set[URIRef] = set(seeds)
    discovered_via: Dict[URIRef, str] = {c: "seed" for c in seeds}

    total_sub = 0
    total_eq = 0
    layers = 0

    while True:
        if layers >= max_layers:
            break

        new_nodes: Set[URIRef] = set()
        new_eq: Set[URIRef] = set()
        new_sub: Set[URIRef] = set()

        for c in list(closure):
            # eq expansions
            for n in eq_adj.get(c, ()):
                if n not in closure:
                    new_nodes.add(n)
                    new_eq.add(n)
            # subclass expansions
            for n in sub_down.get(c, ()):
                if n not in closure:
                    new_nodes.add(n)
                    new_sub.add(n)

        if not new_nodes:
            break

        layers += 1

        # apply guards
        if len(closure) + len(new_nodes) > max_closure:
            # stop early: closure too big
            break

        # add and attribute
        for n in new_nodes:
            if n in closure:
                continue
            closure.add(n)
            if n in new_eq and n not in discovered_via:
                discovered_via[n] = "eq"
            elif n in new_sub and n not in discovered_via:
                discovered_via[n] = "sub"

        # count only *first discovery* by mechanism
        for n in new_eq:
            if discovered_via.get(n) == "eq":
                total_eq += 1
        for n in new_sub:
            if discovered_via.get(n) == "sub":
                total_sub += 1

    return closure, total_sub, total_eq, layers


def inst_factor(entities: Optional[int], cap: int, transform: str) -> float:
    if entities is None:
        return 0.0
    x = float(min(int(entities), cap))
    if transform == "linear":
        return x
    if transform == "sqrt":
        return math.sqrt(x)
    if transform == "log":
        return math.log1p(x)
    raise ValueError(f"Unknown inst transform: {transform}")


def extract_subset_graph(
    shapes_g: Graph,
    selected_shapes: List[URIRef],
    subset_out: str,
    follow_prefixes: List[str],
) -> None:
    """
    Export a coherent subgraph containing:
      - selected shape nodes
      - any referenced shaclshapes.org nodes (property shapes, constraint nodes)
      - blank nodes reachable from them
    """
    out = Graph()
    # preserve namespaces for nicer TTL
    for pfx, ns in shapes_g.namespaces():
        out.bind(pfx, ns)

    q = deque(selected_shapes)
    seen = set(selected_shapes)

    def should_follow(u: URIRef) -> bool:
        s = str(u)
        return any(s.startswith(p) for p in follow_prefixes)

    while q:
        s = q.popleft()
        for p, o in shapes_g.predicate_objects(s):
            out.add((s, p, o))
            if isinstance(o, BNode) and o not in seen:
                seen.add(o)
                q.append(o)
            elif isinstance(o, URIRef) and o not in seen and should_follow(o):
                seen.add(o)
                q.append(o)

    out.serialize(destination=subset_out, format="turtle")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shapes", required=True, help="Path to DBpedia_SHACL.ttl")
    ap.add_argument("--ontology", required=True, help="Path to local dbpedia_ontology.owl")
    ap.add_argument("--top-k", type=int, default=30)

    # Filters / guards (tune these to avoid “too rich”)
    ap.add_argument("--target-prefix", default="http://dbpedia.org/ontology/",
                    help="Only consider sh:targetClass IRIs with this prefix (default dbo:)")
    ap.add_argument("--max-targets-per-shape", type=int, default=1,
                    help="Skip shapes with more dbo targets than this (default 1). Set 0 to disable.")
    ap.add_argument("--min-entities", type=int, default=200)
    ap.add_argument("--max-entities", type=int, default=50000)
    ap.add_argument("--max-props", type=int, default=80)
    ap.add_argument("--max-closure", type=int, default=250)
    ap.add_argument("--max-layers", type=int, default=25)

    # Scoring
    ap.add_argument("--cap-entities", type=int, default=10000)
    ap.add_argument("--inst-transform", choices=["linear", "sqrt", "log"], default="sqrt",
                    help="Avoid huge classes dominating: sqrt/log are usually better than linear.")
    ap.add_argument("--score-mode", choices=["rich", "eq", "balanced"], default="balanced",
                    help="rich=(sub+eq)*inst; eq=(eq)*inst; balanced=(sub+eq)*instFactor with damping.")
    ap.add_argument("--balanced-sub-weight", type=float, default=0.5,
                    help="Extra weight for sub expansion in balanced mode (default 0.5).")

    # Output
    ap.add_argument("--csv-out", default="shapes_ranking.csv")
    ap.add_argument("--subset-out", default="shapes_topk.ttl")
    ap.add_argument("--ontology-cache", default=None,
                    help="Optional pickle cache for ontology index (speeds up reruns).")

    # Skip list (very general classes)
    ap.add_argument("--avoid", nargs="*", default=[
        "http://dbpedia.org/ontology/Thing",
        "http://dbpedia.org/ontology/Agent",
        "http://dbpedia.org/ontology/Person",
        "http://dbpedia.org/ontology/Place",
        "http://dbpedia.org/ontology/Work",
    ], help="Explicit targetClass IRIs to skip.")

    args = ap.parse_args()

    shapes_g = Graph()
    parse_any(shapes_g, args.shapes)

    # Build ontology index restricted to dbo prefix (keeps closure sane)
    sub_down, eq_adj = build_ontology_index(
        args.ontology,
        cache_path=args.ontology_cache,
        class_prefixes=None,
    )

    node_shapes = list(shapes_g.subjects(RDF.type, SH.NodeShape))

    results: List[ShapeStats] = []
    selected_shape_nodes: List[URIRef] = []

    avoid_set = set(args.avoid)

    for s in node_shapes:
        # targets (filter to dbo prefix)
        targets_all = list(shapes_g.objects(s, SH.targetClass))
        targets_dbo = [t for t in targets_all if isinstance(t, URIRef) and str(t).startswith(args.target_prefix)]

        if not targets_dbo:
            continue

        if args.max_targets_per_shape > 0 and len(targets_dbo) > args.max_targets_per_shape:
            continue

        # avoid very general / explicitly blacklisted
        if any(str(t) in avoid_set for t in targets_dbo):
            continue

        # offline instance proxy (void:entities is usually present)
        ent_lit = next(iter(shapes_g.objects(s, VOID.entities)), None)
        entities = None
        if isinstance(ent_lit, Literal):
            try:
                entities = int(str(ent_lit))
            except Exception:
                entities = None

        if entities is not None and (entities < args.min_entities or entities > args.max_entities):
            continue

        # property shapes count
        prop_shapes = len(list(shapes_g.objects(s, SH.property)))
        if prop_shapes > args.max_props:
            continue

        # closure
        seed_set = set(targets_dbo)
        closure, total_sub, total_eq, layers = closure_fixpoint(
            seed_set, sub_down, eq_adj,
            max_closure=args.max_closure,
            max_layers=args.max_layers,
        )

        closure_size = len(closure)
        if closure_size > args.max_closure:
            continue
        if layers > args.max_layers:
            continue

        # scoring with damping on entities
        inst = inst_factor(entities, args.cap_entities, args.inst_transform)

        score_rich_linear = float(1 + total_sub + total_eq) * float(min(entities or 0, args.cap_entities))

        if args.score_mode == "rich":
            score = float(1 + total_sub + total_eq) * inst
        elif args.score_mode == "eq":
            score = float(1 + total_eq) * inst
        else:
            # balanced: still rewards sub+eq, but less dominated by entities
            score = (float(1 + total_eq) * inst) + (args.balanced_sub_weight * float(1 + total_sub) * inst)

        results.append(
            ShapeStats(
                shape=str(s),
                target_classes=";".join(str(t) for t in targets_dbo),
                entities=entities,
                prop_shapes=prop_shapes,
                total_sub=total_sub,
                total_eq=total_eq,
                closure_size=closure_size,
                total_layers=layers,
                score=float(score),
                score_rich_linear=score_rich_linear,
            )
        )

    # Sort + take topK
    results.sort(key=lambda r: (r.score, r.total_eq, r.total_sub), reverse=True)
    top = results[: args.top_k]

    # Write CSV
    with open(args.csv_out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(top[0]).keys()) if top else list(asdict(ShapeStats("", "", None, 0, 0, 0, 0, 0, 0.0, 0.0)).keys()))
        w.writeheader()
        for r in results:
            w.writerow(asdict(r))

    # Build subset TTL (follow shaclshapes.org nodes + blank nodes)
    selected_shape_nodes = [URIRef(r.shape) for r in top]
    extract_subset_graph(
        shapes_g,
        selected_shape_nodes,
        subset_out=args.subset_out,
        follow_prefixes=[
            "http://shaclshapes.org/",
        ],
    )

    # Print a compact summary
    print(f"Shapes scanned: {len(node_shapes)}")
    print(f"Shapes kept after filters: {len(results)}")
    print(f"Top-{len(top)} written to: {args.subset_out}")
    print(f"Full ranking written to: {args.csv_out}")
    print("")
    for i, r in enumerate(top, 1):
        print(f"{i:02d} score={r.score:.3f} ent={r.entities} props={r.prop_shapes} "
              f"sub={r.total_sub} eq={r.total_eq} closure={r.closure_size} layers={r.total_layers} "
              f"shape={r.shape.rsplit('/',1)[-1]} targets={r.target_classes}")


if __name__ == "__main__":
    main()
