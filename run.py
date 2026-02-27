from importlib import reload
from rdflib import Graph, Namespace
from pyshacl import validate
import time
import sys
from prettytable import PrettyTable
import numpy as np

from reSHACL.re_shacl import merged_graph
from reSHACL.re_shacl_no_tc import merged_graph_no_tc
from reSHACL.re_shacl_virtuoso import merged_graph_virtuoso

import os
import logging

from tc_engine.engine_virtuoso import export_work_graph_ttl, OUT_TTL, reset_work_graph, copy_shapes_to_work  

DBO = Namespace("http://dbpedia.org/ontology/")
sys.path.insert(0, sys.path[0] + "/../")

if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding("utf-8")


def ns_to_s(ns: int) -> float:
    return ns / 1_000_000_000.0


def mean_std(arr):
    return float(np.mean(arr)), float(np.std(arr))


def get_tc_ns_from_timing(timing: dict) -> int:
    """
    Unified TC extraction across methods.
    - new engines: tc_engine_only_ns or tc_engine_expand_and_rewrite_ns
    - old reshacl: tc_old_only_ns or tc_only_ns
    """
    if not timing:
        return 0
    for k in ("tc_only_ns", "tc_old_only_ns", "tc_engine_only_ns", "tc_engine_expand_and_rewrite_ns"):
        if k in timing:
            return int(timing[k])
    # fallback: sum legacy keys if present
    return int(timing.get("tc_subclass_expand_only_ns", 0)) + int(timing.get("tc_merge_only_ns", 0))


def call_merged(fn, *args, **kwargs):
    """
    Supports:
      - (fused_graph, same_dic, shapes)
      - (fused_graph, same_dic, shapes, timing)
    Returns always 4 values, timing defaults to {}.
    """
    res = fn(*args, **kwargs)
    if isinstance(res, tuple) and len(res) == 4:
        return res
    if isinstance(res, tuple) and len(res) == 3:
        fused_graph1, same_dic1, shapes = res
        return fused_graph1, same_dic1, shapes, {}
    raise RuntimeError("Unexpected merged_graph return signature")


def check_directory_exists_otherwise_create(directory):
    if not os.path.isdir(directory):
        folder_names = directory.split("/")
        folder_name = ""
        for name in folder_names:
            folder_name += name + "/"
            if not os.path.isdir(folder_name):
                os.mkdir(folder_name)
                print(f"Created folder: {folder_name}")


def load_base_graphs(dataset_uri: str, shapes_graph_uri: str, ontology_uri: str):
    logging.getLogger("rdflib").setLevel(logging.ERROR)

    base_g = Graph()
    base_g.parse(dataset_uri)
    if ontology_uri:
        base_g.parse(ontology_uri, format="xml")

    base_sg = Graph()
    base_sg.parse(shapes_graph_uri)
    base_sg.bind("dbo", DBO)

    ont_g = Graph()
    if ontology_uri:
        ont_g.parse(ontology_uri, format="xml")

    return base_g, base_sg, ont_g


def clone_graph(src: Graph) -> Graph:
    g2 = Graph()
    for prefix, ns in src.namespace_manager.namespaces():
        g2.bind(prefix, ns)
    for t in src.triples((None, None, None)):
        g2.add(t)
    return g2


# ----------------------------
# NEW: Engine-SPARQL fair prep
# ----------------------------
def run_engine_sparql_build(g: Graph) -> tuple[Graph, dict, Graph]:
    """
    Pipeline:
      1) export_work_graph_ttl -> counted as TC-only
      2) parse TTL -> RDFlib     -> NOT counted anywhere
      3) merged_graph_virtuoso(shacl_graph=parsed) -> counted as merge-only
    Build time = TC-only + merge-only (parse excluded).
    Returns:
      fused_graph, timing, shapes_graph_used_for_validation
    """
    timing: dict[str, int | float] = {}

    # 1) TC-only: export
    reset_work_graph()
    copy_shapes_to_work()
    t_tc0 = time.perf_counter_ns()
    #for one query method
    # exported_path = export_work_graph_ttl(OUT_TTL, method="updated_shapes_graph")
    exported_path = export_work_graph_ttl(OUT_TTL)
    t_tc1 = time.perf_counter_ns()
    tc_ns = t_tc1 - t_tc0

    timing["tc_engine_only_ns"] = tc_ns
    timing["tc_engine_only_s"] = tc_ns / 1e9

    
    shacl_graph_engine = Graph()
    shacl_graph_engine.parse(exported_path, format="turtle")
    

    # 3) merge-only
    t_m0 = time.perf_counter_ns()
    fused_graph1, same_dic1, shapes, merge_timing = call_merged(
        merged_graph_virtuoso,
        g,
        shacl_graph=shacl_graph_engine,
        data_graph_format="turtle",
        shacl_graph_format="turtle",
    )
    t_m1 = time.perf_counter_ns()
    merge_ns = t_m1 - t_m0

    timing["merge_only_ns"] = merge_ns
    timing["merge_only_s"] = merge_ns / 1e9

    # BUILD definition you want: export + merged_graph_virtuoso (parse excluded)
    build_ns = tc_ns + merge_ns
    timing["build_defined_ns"] = build_ns
    timing["build_defined_s"] = build_ns / 1e9

    # We return the shapes graph we actually want pyshacl to validate with:
    # use `shapes` returned by merged_graph_virtuoso (if it’s the rewritten one),
    # otherwise use shacl_graph_engine. Your current code expects `shapes` for validation.
    return fused_graph1, timing, shapes



def build_call(method_id: str, g: Graph, sg: Graph, ont_g: Graph):
    if method_id == "reshacl":
        return call_merged(
            merged_graph,
            g,
            shacl_graph=sg,
            data_graph_format="turtle",
            shacl_graph_format="turtle",
        )

    if method_id == "engine_rdflib":
        return call_merged(
            merged_graph_no_tc,
            g,
            ont_g, 
            shacl_graph=sg,
            data_graph_format="turtle",
            shacl_graph_format="turtle",
        )

    raise ValueError(f"Unknown method_id: {method_id}")


def benchmark_method(
    method_label: str,
    method_id: str,
    dataset_name: str,
    base_g: Graph,
    base_sg: Graph,
    ont_g: Graph,
    inference_method="none",
    runs=3,
    verbose_iter=True
):
    table = PrettyTable([
        "Method",
        "|G| before", "|G| after",
        "|S| before", "|S| after",
        "Avg total (s)", "Std total",
        "Avg build (s)", "Std build",
        "Avg valid (s)", "Std valid",
        "Avg TC (s)",    "Std TC",
        "Conform", "#Violation"
    ])

    total_s, build_s, valid_s, tc_s = [], [], [], []
    g_before_list, g_after_list = [], []
    s_before_list, s_after_list = [], []

    last_conform, last_v_g, last_v_t = None, None, None

    for i in range(runs):
        g = clone_graph(base_g)
        sg = clone_graph(base_sg)

        g_before = len(g)  # includes ontology because base_g includes it
        s_before = len(sg)

        if method_id == "engine_sparql":
            fused_graph1, timing, shapes = run_engine_sparql_build(g)
            b_s = float(timing["build_defined_s"])
        else:
            t0 = time.perf_counter_ns()
            fused_graph1, same_dic1, shapes, timing = build_call(method_id, g, sg, ont_g)
            t1 = time.perf_counter_ns()
            b_s = ns_to_s(t1 - t0)


        g_after = len(fused_graph1)
        s_after = len(shapes)

        g_before_list.append(g_before)
        g_after_list.append(g_after)
        s_before_list.append(s_before)
        s_after_list.append(s_after)

        # VALIDATE
        shapes.bind("dbo", DBO)
        t2 = time.perf_counter_ns()
        conform, v_g, v_t = validate(fused_graph1, shacl_graph=shapes, inference=inference_method)
        t3 = time.perf_counter_ns()
        v_s = ns_to_s(t3 - t2)

        # TC-only (engine_sparql gets this from prepare_engine_sparql_shapes_as_rdflib timing)
        tc_ns = get_tc_ns_from_timing(timing)
        tc_sec = ns_to_s(tc_ns)

        tot = b_s + v_s

        total_s.append(tot)
        build_s.append(b_s)
        valid_s.append(v_s)
        tc_s.append(tc_sec)

        last_conform, last_v_g, last_v_t = conform, v_g, v_t

        if verbose_iter:
            print(
                f" [{method_label}] run {i+1}/{runs}  "
                f"build={b_s:.6f}s  valid={v_s:.6f}s  total={tot:.6f}s  tc={tc_sec:.6f}s"
            )

    # stats
    m_total, sd_total = mean_std(total_s)
    m_build, sd_build = mean_std(build_s)
    m_valid, sd_valid = mean_std(valid_s)
    m_tc,    sd_tc    = mean_std(tc_s)
    m_g_before, sd_g_before = mean_std(g_before_list)
    m_g_after,  sd_g_after  = mean_std(g_after_list)
    m_s_before, sd_s_before = mean_std(s_before_list)
    m_s_after,  sd_s_after  = mean_std(s_after_list)
    m_g_before_i = int(round(m_g_before))
    m_g_after_i  = int(round(m_g_after))
    m_s_before_i = int(round(m_s_before))
    m_s_after_i  = int(round(m_s_after))

    # violations (use last report graph)
    result_query = """
    PREFIX sh: <http://www.w3.org/ns/shacl#>
    SELECT ?v
    WHERE {
        ?s sh:result ?v
    }"""

    result = last_v_g.query(result_query)
    viol_count = len(result)

    print(f'[{method_label}]=============================')
    print(f' Avg total: {m_total:.6f}s  Std: {sd_total:.6f}')
    print(f' Avg build: {m_build:.6f}s  Std: {sd_build:.6f}')
    print(f' Avg valid: {m_valid:.6f}s  Std: {sd_valid:.6f}')
    print(f' Avg TC:    {m_tc:.6f}s  Std: {sd_tc:.6f}')
    print(f' #Violation: {viol_count}')

    
    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/violationGraph/")
    last_v_g.serialize(destination=f"Outputs/{dataset_name}/violationGraph/{method_label}_results.ttl")

    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/validationReports/")
    with open(f"Outputs/{dataset_name}/validationReports/{method_label}_results.txt", "w", encoding="utf-8") as f:
        f.write(last_v_t)

    table.add_row([
        method_label,
        m_g_before_i, m_g_after_i,
        m_s_before_i, m_s_after_i,
        m_total, sd_total,
        m_build, sd_build,
        m_valid, sd_valid,
        m_tc, sd_tc,
        last_conform,
        viol_count
    ])

    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/")
    with open(f"Outputs/{dataset_name}/RunTimeResults.txt", "a+", encoding="utf-8") as file_table:
        file_table.write(str(table) + "\n")

    print(f" |G|: {m_g_before_i} -> {m_g_after_i}  (Δ {m_g_after_i - m_g_before_i:+d})")
    print(f" |S|: {m_s_before_i} -> {m_s_after_i}  (Δ {m_s_after_i - m_s_before_i:+d})")
    print(table)


def run_experiment(dataset_name, dataset_uri, shapes_graph_uri, ontology_uri):
    print("***** Loading the data graph *****")
    print("***** Loading the ontology *****" if ontology_uri else "***** Skipping ontology *****")
    print("***** Loading the shapes graph *****")

    base_g, base_sg, ont_g = load_base_graphs(dataset_uri, shapes_graph_uri, ontology_uri)

    print("***** Preheating *****")
    # for _ in range(5):
    #     g0 = clone_graph(base_g)
    #     sg0 = clone_graph(base_sg)
    #     validate(g0, shacl_graph=sg0, inference="none")

    print(f"***** START VALIDATION ON [{dataset_name}] *****")

    # benchmark_method(
    #     method_label="original",
    #     method_id="reshacl",
    #     dataset_name=dataset_name,
    #     base_g=base_g,
    #     base_sg=base_sg,
    #     ont_g=ont_g,
    #     inference_method="none",
    #     runs=1,
    #     verbose_iter=True,
    # )

    benchmark_method(
        method_label="RDFlib",
        method_id="engine_rdflib",
        dataset_name=dataset_name,
        base_g=base_g,
        base_sg=base_sg,
        ont_g=ont_g,
        inference_method="none",
        runs=5,
        verbose_iter=True,
    )

    benchmark_method(
        method_label="original",
        method_id="reshacl",
        dataset_name=dataset_name,
        base_g=base_g,
        base_sg=base_sg,
        ont_g=ont_g,
        inference_method="none",
        runs=5,
        verbose_iter=True,
    )


    benchmark_method(
        method_label="SPARQL",
        method_id="engine_sparql",
        dataset_name=dataset_name,
        base_g=base_g,
        base_sg=base_sg,
        ont_g=ont_g,
        inference_method="none",
        runs=5,
        verbose_iter=True,
    )


if __name__ == "__main__":
    run_experiment(
        dataset_name="small",
        dataset_uri="source/Datasets/small.ttl",
        shapes_graph_uri="C:\\Users\\mazek.ZZIRKELL\\reshacl_thesis\\source\\ShapesGraphs\\DBpedia_SHACL_selected30.ttl",
        ontology_uri="source/Ontologies/dbpedia_ontology.owl",
    )

    run_experiment(
        dataset_name="medium",
        dataset_uri="source/Datasets/medium.ttl",
        shapes_graph_uri="C:\\Users\\mazek.ZZIRKELL\\reshacl_thesis\\source\\ShapesGraphs\\DBpedia_SHACL_selected30.ttl",
        ontology_uri="reshacl_thesis/source/Ontologies/dbpedia_ontology.owl",
    )

    run_experiment(
        dataset_name="large",
        dataset_uri="source/Datasets/large.ttl",
        shapes_graph_uri="C:\\Users\\mazek.ZZIRKELL\\reshacl_thesis\\source\\ShapesGraphs\\DBpedia_SHACL_selected30.ttl",
        ontology_uri="reshacl_thesis/source/Ontologies/dbpedia_ontology.owl",
    )