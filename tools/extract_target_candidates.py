import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import rdflib
from rdflib import Graph, Namespace
from rdflib.namespace import RDF

# -----------------------------
# Config
# -----------------------------
ENDPOINT = "https://dbpedia.org/sparql"

SHAPES_PATH = Path("source/ShapesGraphs/DBpedia_SHACL.ttl")
OUT_DIR = Path("source/DatasetPrep")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = OUT_DIR / "targetclass_metrics.jsonl"   # append-only; used for resume
OUT_CSV = OUT_DIR / "targetclass_metrics.csv"

CAP_INST = 10_000               # instance proxy cap
VALUES_BATCH = 60               # VALUES batch for class IRIs
SLEEP_S = 0.15                  # politeness sleep per remote call

MAX_ITERS = 50                  # max BFS layers iterations (safety)
MAX_SEEN_CLASSES = 50_000       # safety cap to avoid runaway closure

# If closure becomes huge, we truncate class list for instance proxy query.
MAX_CLASSES_IN_INST_QUERY = 400

SH = Namespace("http://www.w3.org/ns/shacl#")

# -----------------------------
# Networking helpers
# -----------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

def sparql_select(query: str) -> List[dict]:
    """
    GET SELECT results as JSON.
    DBpedia/Virtuoso may return 206 for partial results; for our small targeted queries, parse anyway.
    """
    r = SESSION.get(
        ENDPOINT,
        params={"query": query, "format": "json"},
        headers={"Accept": "application/sparql-results+json"},
        timeout=120,
    )
    if r.status_code not in (200, 206):
        r.raise_for_status()
    return r.json()["results"]["bindings"]

def chunks(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

# -----------------------------
# Shapes parsing
# -----------------------------
def load_unique_target_classes() -> List[str]:
    if not SHAPES_PATH.exists():
        raise FileNotFoundError(f"Shapes TTL not found: {SHAPES_PATH.resolve()}")

    g = Graph()
    g.parse(SHAPES_PATH.as_posix(), format="turtle")
    targets = sorted({str(o) for _, _, o in g.triples((None, SH.targetClass, None))})
    return targets

# -----------------------------
# Remote ontology neighborhood queries
# -----------------------------
def fetch_equiv_neighbors(classes: Set[str]) -> Set[str]:
    """
    Return all ?x such that:
      c owl:equivalentClass x OR x owl:equivalentClass c OR
      c owl:sameAs x OR x owl:sameAs c
    for any c in classes.
    """
    out: Set[str] = set()
    clist = list(classes)
    for batch in chunks(clist, VALUES_BATCH):
        vals = " ".join(f"<{c}>" for c in batch)
        q = f"""
        SELECT DISTINCT ?x WHERE {{
          VALUES ?c {{ {vals} }}
          {{
            ?c <http://www.w3.org/2002/07/owl#equivalentClass> ?x .
          }} UNION {{
            ?x <http://www.w3.org/2002/07/owl#equivalentClass> ?c .
          }} UNION {{
            ?c <http://www.w3.org/2002/07/owl#sameAs> ?x .
          }} UNION {{
            ?x <http://www.w3.org/2002/07/owl#sameAs> ?c .
          }}
          FILTER(isIRI(?x))
        }}
        """
        rows = sparql_select(q)
        out |= {row["x"]["value"] for row in rows}
        time.sleep(SLEEP_S)
    return out

def fetch_subclasses_of(classes: Set[str]) -> Set[str]:
    """
    Return all ?sub such that:
      ?sub rdfs:subClassOf c
    for any c in classes.
    """
    out: Set[str] = set()
    clist = list(classes)
    for batch in chunks(clist, VALUES_BATCH):
        vals = " ".join(f"<{c}>" for c in batch)
        q = f"""
        SELECT DISTINCT ?sub WHERE {{
          VALUES ?c {{ {vals} }}
          ?sub <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?c .
          FILTER(isIRI(?sub))
        }}
        """
        rows = sparql_select(q)
        out |= {row["sub"]["value"] for row in rows}
        time.sleep(SLEEP_S)
    return out

def instance_proxy_for_classes(classes: Set[str], cap: int = CAP_INST) -> int:
    """
    Return <= cap distinct instances of ANY class in `classes`.
    Uses one VALUES query. If class list is huge, truncate to keep responsiveness.
    Proxy meaning:
      inst_proxy == cap  => "at least cap instances"
    """
    clist = list(classes)
    if len(clist) > MAX_CLASSES_IN_INST_QUERY:
        clist = clist[:MAX_CLASSES_IN_INST_QUERY]

    vals = " ".join(f"<{c}>" for c in clist)
    q = f"""
    SELECT DISTINCT ?s WHERE {{
      VALUES ?c {{ {vals} }}
      ?s a ?c .
    }}
    ORDER BY ?s
    LIMIT {cap}
    """
    rows = sparql_select(q)
    return len(rows)

# -----------------------------
# Metrics computation (deep closure)
# -----------------------------
@dataclass
class Metrics:
    cls: str
    total_sub: int
    total_eq: int
    total_layers: int
    closure_size: int
    inst_proxy: int
    score_rich: int
    score_layers: int
    status: str
    error: str

DBO_PREFIX = "http://dbpedia.org/ontology/"

def only_dbo(iris: Set[str]) -> Set[str]:
    return {x for x in iris if x.startswith(DBO_PREFIX)}

def compute_closure_metrics(seed_class: str) -> Metrics:
    """
    Fixpoint-iteration layers (Virtuoso semantics), but implemented fast using delta expansion.
    total_layers = number of rounds until no new eq/sub classes are found.
    """
    try:
        seen: Set[str] = {seed_class}
        delta: Set[str] = {seed_class}

        total_eq = 0
        total_sub = 0
        total_layers = 0  # fixpoint iterations

        for it in range(1, MAX_ITERS + 1):
            if len(seen) > MAX_SEEN_CLASSES:
                return Metrics(
                    cls=seed_class,
                    total_sub=total_sub,
                    total_eq=total_eq,
                    total_layers=it - 1,
                    closure_size=len(seen),
                    inst_proxy=0,
                    score_rich=0,
                    score_layers=it - 1,
                    status="capped",
                    error=f"closure exceeded MAX_SEEN_CLASSES={MAX_SEEN_CLASSES}",
                )

            # Expand from delta only (new classes from last round)
            eq_new = only_dbo(fetch_equiv_neighbors(delta)) - seen
            sub_new = only_dbo(fetch_subclasses_of(delta)) - seen


            if not eq_new and not sub_new:
                total_layers = it - 1
                break

            new_delta: Set[str] = set()

            if eq_new:
                seen |= eq_new
                new_delta |= eq_new
                total_eq += len(eq_new)

            if sub_new:
                seen |= sub_new
                new_delta |= sub_new
                total_sub += len(sub_new)

            delta = new_delta
            total_layers = it  # we completed this round

        inst_proxy = instance_proxy_for_classes(seen, CAP_INST)
        score_rich = (1 + total_eq + total_sub) * inst_proxy
        score_layers = total_layers

        status = "ok"
        error = ""
        if total_layers == MAX_ITERS:
            status = "capped"
            error = f"did not converge within MAX_ITERS={MAX_ITERS}"

        return Metrics(
            cls=seed_class,
            total_sub=total_sub,
            total_eq=total_eq,
            total_layers=total_layers,
            closure_size=len(seen),
            inst_proxy=inst_proxy,
            score_rich=score_rich,
            score_layers=score_layers,
            status=status,
            error=error,
        )

    except Exception as e:
        return Metrics(
            cls=seed_class,
            total_sub=0,
            total_eq=0,
            total_layers=0,
            closure_size=1,
            inst_proxy=0,
            score_rich=0,
            score_layers=0,
            status="error",
            error=str(e),
        )

# -----------------------------
# Resume support
# -----------------------------
def load_done_from_jsonl(path: Path) -> Dict[str, Metrics]:
    done: Dict[str, Metrics] = {}
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            m = Metrics(**obj)
            done[m.cls] = m
    return done

def append_metric_jsonl(path: Path, m: Metrics) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(m), ensure_ascii=False) + "\n")

def write_csv(path: Path, metrics: List[Metrics]) -> None:
    import csv
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "class",
            "total_sub",
            "total_eq",
            "inst_proxy_cap10k",
            "total_layers",
            "closure_size",
            "score_rich",
            "score_layers",
            "status",
            "error",
        ])
        for m in metrics:
            w.writerow([
                m.cls,
                m.total_sub,
                m.total_eq,
                m.inst_proxy,
                m.total_layers,
                m.closure_size,
                m.score_rich,
                m.score_layers,
                m.status,
                m.error,
            ])

# -----------------------------
# Main
# -----------------------------
def main():
    targets = load_unique_target_classes()
    print(f"Unique sh:targetClass values: {len(targets)}")

    done = load_done_from_jsonl(OUT_JSONL)
    print(f"Already computed (resume): {len(done)}")

    all_metrics: List[Metrics] = []

    for idx, cls in enumerate(targets, 1):
        if cls in done:
            all_metrics.append(done[cls])
            continue

        print(f"[{idx}/{len(targets)}] Computing deep closure metrics for: {cls}")
        m = compute_closure_metrics(cls)
        append_metric_jsonl(OUT_JSONL, m)
        all_metrics.append(m)

        # Progress print in your requested format
        print(
            f"  total_sub={m.total_sub} total_eq={m.total_eq} "
            f"inst_proxy={m.inst_proxy} total_layers={m.total_layers} "
            f"score_rich={m.score_rich} score_layers={m.score_layers} status={m.status}"
        )

        # periodic CSV flush so you always have something
        if idx % 10 == 0:
            write_csv(OUT_CSV, sorted(all_metrics, key=lambda x: x.cls))
            print(f"  [checkpoint] wrote CSV: {OUT_CSV.resolve()}")

    # Final write
    write_csv(OUT_CSV, sorted(all_metrics, key=lambda x: x.cls))
    print(f"\nFinal CSV written: {OUT_CSV.resolve()}")
    print(f"Final JSONL written: {OUT_JSONL.resolve()}")

    ok_metrics = [m for m in all_metrics if m.status in ("ok", "capped")]
    ok_metrics.sort(key=lambda x: x.score_rich, reverse=True)

    print("\n=== TOP 30 by score_rich (richness) ===")
    for m in ok_metrics[:30]:
        cap_note = ">=CAP" if m.inst_proxy == CAP_INST else str(m.inst_proxy)
        print(
            f"{m.cls} | total_sub={m.total_sub} total_eq={m.total_eq} "
            f"inst_proxy={cap_note} layers={m.total_layers} closure={m.closure_size} score_rich={m.score_rich}"
        )

    ok_metrics_by_layers = sorted(ok_metrics, key=lambda x: (x.total_layers, x.closure_size), reverse=True)
    print("\n=== TOP 30 by total_layers (depth) ===")
    for m in ok_metrics_by_layers[:30]:
        cap_note = ">=CAP" if m.inst_proxy == CAP_INST else str(m.inst_proxy)
        print(
            f"{m.cls} | layers={m.total_layers} total_sub={m.total_sub} total_eq={m.total_eq} "
            f"inst_proxy={cap_note} closure={m.closure_size} score_rich={m.score_rich}"
        )

if __name__ == "__main__":
    main()
