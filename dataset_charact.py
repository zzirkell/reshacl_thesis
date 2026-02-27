from __future__ import annotations

import csv
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL
from rdflib import Namespace

SH = Namespace("http://www.w3.org/ns/shacl#")


# -----------------------------
# CONFIG: change paths here
# -----------------------------
DATASETS = {
    "small": Path(r"C:\Users\mazek.ZZIRKELL\reshacl_thesis\source\Datasets\small.ttl"),
    "medium": Path(r"C:\Users\mazek.ZZIRKELL\reshacl_thesis\source\Datasets\medium.ttl"),
    "large": Path(r"C:\Users\mazek.ZZIRKELL\reshacl_thesis\source\Datasets\large.ttl"),
}

SHAPES_PATH = Path(r"C:\Users\mazek.ZZIRKELL\reshacl_thesis\source\ShapesGraphs\DBpedia_SHACL.ttl")
# If you extracted a smaller shapes subset for experiments, point to that instead:
# SHAPES_PATH = Path(r"...\source\DatasetPrep\Corpora\shapes_subset.ttl")

OUT_CSV = Path(r"C:\Users\mazek.ZZIRKELL\reshacl_thesis\source\Datasets\dataset_profile.csv")


# -----------------------------
# Helpers
# -----------------------------
def load_targets_from_shapes(shapes_path: Path) -> Set[str]:
    g = Graph()
    g.parse(shapes_path.as_posix(), format="turtle")
    targets = {str(o) for _, _, o in g.triples((None, SH.targetClass, None))}
    return targets


@dataclass
class DatasetStats:
    dataset: str
    total_triples: int

    distinct_subjects: int
    distinct_predicates: int
    distinct_objects: int

    distinct_iris: int
    distinct_literals: int

    rdf_type_triples: int
    rdf_type_to_targetclass_triples: int
    focus_nodes_count: int
    focus_coverage_ratio: float  # focus_nodes / distinct_iris

    subClassOf_triples: int
    equivalentClass_triples: int
    sameAs_triples: int

    top_predicates: str  # compact string
    top_targetclasses: str  # compact string


def analyze_graph(g: Graph, target_classes: Set[str], topk: int = 10) -> Tuple[DatasetStats, Dict[str, int]]:
    # core counts
    total_triples = len(g)

    subj_set: Set[str] = set()
    pred_set: Set[str] = set()
    obj_set: Set[str] = set()

    iri_nodes: Set[str] = set()
    lit_nodes: Set[str] = set()

    pred_counter = Counter()

    rdf_type_triples = 0
    rdf_type_to_target = 0
    focus_nodes: Set[str] = set()
    targetclass_instance_counts = Counter()

    sub_triples = 0
    eq_triples = 0
    same_triples = 0

    for s, p, o in g:
        s_str = str(s)
        p_str = str(p)
        o_str = str(o)

        subj_set.add(s_str)
        pred_set.add(p_str)
        obj_set.add(o_str)

        if isinstance(s, URIRef):
            iri_nodes.add(s_str)
        if isinstance(o, URIRef):
            iri_nodes.add(o_str)
        if isinstance(o, Literal):
            lit_nodes.add(o_str)

        pred_counter[p_str] += 1

        # ontology relations (if present)
        if p == RDFS.subClassOf:
            sub_triples += 1
        elif p == OWL.equivalentClass:
            eq_triples += 1
        elif p == OWL.sameAs:
            same_triples += 1

        # typing / focus nodes
        if p == RDF.type:
            rdf_type_triples += 1
            if o_str in target_classes:
                rdf_type_to_target += 1
                focus_nodes.add(s_str)
                targetclass_instance_counts[o_str] += 1

    distinct_iris = len(iri_nodes)
    focus_cov = (len(focus_nodes) / distinct_iris) if distinct_iris else 0.0

    # compact strings for reporting
    top_pred = ", ".join([f"{k.split('/')[-1]}={v}" for k, v in pred_counter.most_common(topk)])
    top_tc = ", ".join([f"{k.split('/')[-1]}={v}" for k, v in targetclass_instance_counts.most_common(min(15, topk))])

    stats = DatasetStats(
        dataset="",
        total_triples=total_triples,
        distinct_subjects=len(subj_set),
        distinct_predicates=len(pred_set),
        distinct_objects=len(obj_set),
        distinct_iris=distinct_iris,
        distinct_literals=len(lit_nodes),
        rdf_type_triples=rdf_type_triples,
        rdf_type_to_targetclass_triples=rdf_type_to_target,
        focus_nodes_count=len(focus_nodes),
        focus_coverage_ratio=focus_cov,
        subClassOf_triples=sub_triples,
        equivalentClass_triples=eq_triples,
        sameAs_triples=same_triples,
        top_predicates=top_pred,
        top_targetclasses=top_tc,
    )

    return stats, dict(targetclass_instance_counts)


def load_graph(path: Path) -> Graph:
    if not path.exists():
        raise FileNotFoundError(path)
    g = Graph()
    g.parse(path.as_posix(), format="turtle")
    return g


def write_csv(path: Path, rows: List[DatasetStats]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))


def pretty_print(rows: List[DatasetStats]) -> None:
    # very readable console output for meeting
    for r in rows:
        print("\n" + "=" * 80)
        print(f"DATASET: {r.dataset}")
        print("=" * 80)
        print(f"|G| total triples: {r.total_triples:,}")
        print(f"Distinct subjects/predicates/objects: {r.distinct_subjects:,} / {r.distinct_predicates:,} / {r.distinct_objects:,}")
        print(f"Distinct IRI nodes: {r.distinct_iris:,}")
        print(f"Distinct literals: {r.distinct_literals:,}")

        print("\nTarget-class coverage (from shapes):")
        print(f"rdf:type triples total: {r.rdf_type_triples:,}")
        print(f"rdf:type triples to target classes: {r.rdf_type_to_targetclass_triples:,}")
        print(f"Focus nodes (#entities typed as target class): {r.focus_nodes_count:,}")
        print(f"Focus coverage ratio: {r.focus_coverage_ratio:.4f}")

        print("\nOntology-ish relations present in the dataset (often 0 in pure data subsets):")
        print(f"rdfs:subClassOf: {r.subClassOf_triples:,}")
        print(f"owl:equivalentClass: {r.equivalentClass_triples:,}")
        print(f"owl:sameAs: {r.sameAs_triples:,}")

        print("\nTop predicates:")
        print(r.top_predicates)

        if r.top_targetclasses:
            print("\nTop target classes by instance count:")
            print(r.top_targetclasses)
        else:
            print("\nTop target classes by instance count: (none found in data)")

    print("\n" + "-" * 80)


def main():
    # 1) load target classes from shapes
    print(f"Loading shapes: {SHAPES_PATH}")
    target_classes = load_targets_from_shapes(SHAPES_PATH)
    print(f"Loaded {len(target_classes)} unique sh:targetClass IRIs\n")

    # 2) analyze each dataset
    results: List[DatasetStats] = []
    for name, path in DATASETS.items():
        print(f"Loading dataset: {name} -> {path}")
        g = load_graph(path)
        stats, _tc_counts = analyze_graph(g, target_classes, topk=10)
        stats.dataset = name
        results.append(stats)

    # 3) print + save CSV
    pretty_print(results)
    write_csv(OUT_CSV, results)
    print(f"Saved CSV: {OUT_CSV}")


if __name__ == "__main__":
    main()