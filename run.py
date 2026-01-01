from importlib import reload
from rdflib import Graph, Namespace
from pyshacl import validate
import time
import sys
from prettytable import PrettyTable
import numpy as np
from reSHACL.re_shacl import merged_graph
from reSHACL.re_shacl_no_tc import merged_graph_no_tc
from reSHACL.re_shacl_no_tc_sparql import merged_graph_no_tc_sparql
import os
import logging

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
            ont_g,  # <-- ontology Graph object
            shacl_graph=sg,
            data_graph_format="turtle",
            shacl_graph_format="turtle",
        )

    if method_id == "engine_sparql":
        return call_merged(
            merged_graph_no_tc_sparql,
            g,
            ont_g,  # <-- ontology Graph object
            shacl_graph=sg,
            data_graph_format="turtle",
            shacl_graph_format="turtle",
        )

    raise ValueError(f"Unknown method_id: {method_id}")

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
        # Recursively make the folders
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
    # preserve prefixes
    for prefix, ns in src.namespace_manager.namespaces():
        g2.bind(prefix, ns)
    for t in src.triples((None, None, None)):
        g2.add(t)
    return g2



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
    """
    Measures (excluding preheating):
      - total = build + validate
      - build only (merged_graph*)
      - validate only (pyshacl.validate)
      - tc_only (from timing dict if provided)
    Keeps violation output logic unchanged (uses last run's v_g / v_t).
    """
    table = PrettyTable([
        "Method",
        "Avg total (s)", "Std total",
        "Avg build (s)", "Std build",
        "Avg valid (s)", "Std valid",
        "Avg TC (s)",    "Std TC",
        "Conform", "#Violation"
    ])

    total_s, build_s, valid_s, tc_s = [], [], [], []

    last_conform, last_v_g, last_v_t = None, None, None

    for i in range(runs):
        g = clone_graph(base_g)
        sg = clone_graph(base_sg)

        # BUILD
        t0 = time.perf_counter_ns()
        fused_graph1, same_dic1, shapes, timing = build_call(method_id, g, sg, ont_g)
        t1 = time.perf_counter_ns()
        b_s = ns_to_s(t1 - t0)

        # VALIDATE
        shapes.bind("dbo", DBO)
        t2 = time.perf_counter_ns()
        conform, v_g, v_t = validate(fused_graph1, shacl_graph=shapes, inference=inference_method)
        t3 = time.perf_counter_ns()
        v_s = ns_to_s(t3 - t2)

        # TC-only
        tc_ns = get_tc_ns_from_timing(timing)
        tc_sec = ns_to_s(tc_ns)

        # --- TOTAL ---
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

    # violations (same logic as you had; use last report graph)
    result_query = """
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

    # save reports (same behavior)
    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/violationGraph/")
    last_v_g.serialize(destination=f"Outputs/{dataset_name}/violationGraph/{method_label}_results.ttl")

    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/validationReports/")
    with open(f"Outputs/{dataset_name}/validationReports/{method_label}_results.txt", "w", encoding="utf-8") as f:
        f.write(last_v_t)

    # table row
    table.add_row([
        method_label,
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

    print(table)


def run_experiment(dataset_name, dataset_uri, shapes_graph_uri, ontology_uri):
    print("***** Loading the data graph *****")
    print("***** Loading the ontology *****" if ontology_uri else "***** Skipping ontology *****")
    print("***** Loading the shapes graph *****")

    base_g, base_sg, ont_g = load_base_graphs(dataset_uri, shapes_graph_uri, ontology_uri)

    # Preheat (excluded from measurement)
    print("***** Preheating *****")
    for _ in range(5):
        g0 = clone_graph(base_g)
        sg0 = clone_graph(base_sg)
        validate(g0, shacl_graph=sg0, inference="none")

    print(f"***** START VALIDATION ON [{dataset_name}] *****")

    # Old ReSHACL
    benchmark_method(
        method_label="ReSHACL",
        method_id="reshacl",
        dataset_name=dataset_name,
        base_g=base_g,
        base_sg=base_sg,
        ont_g=ont_g,
        inference_method="none",
        runs=10,
        verbose_iter=True,
    )

    # Engine (rdflib)
    benchmark_method(
        method_label="ReSHACL+Engine-RDFlib",
        method_id="engine_rdflib",
        dataset_name=dataset_name,
        base_g=base_g,
        base_sg=base_sg,
        ont_g=ont_g,
        inference_method="none",
        runs=10,
        verbose_iter=True,
    )

    # Engine (sparql) — later you can tune
    benchmark_method(
        method_label="ReSHACL+Engine-SPARQL",
        method_id="engine_sparql",
        dataset_name=dataset_name,
        base_g=base_g,
        base_sg=base_sg,
        ont_g=ont_g,
        inference_method="none",
        runs=0,
        verbose_iter=True,
    )



if __name__ == "__main__":
    # run_experiment(
    #     dataset_name="test_violations",
    #     dataset_uri="test_violations/data.ttl",
    #     shapes_graph_uri="test_violations/shapes.ttl",
    #     ontology_uri="test_violations/ont.owl",
    # )

    # run_experiment(
    #     dataset_name="EnDe-Lite50",
    #     dataset_uri="source/Datasets/EnDe-Lite50(without_Ontology).ttl",
    #     shapes_graph_uri="source/ShapesGraphs/Shape_30.ttl",
    #     ontology_uri="source/dbpedia_ontology.owl",
    # )

    run_experiment(
        dataset_name="EnDe-Lite100",
        dataset_uri="source/Datasets/EnDe-Lite100(without_Ontology).ttl",
        shapes_graph_uri="source/ShapesGraphs/Shape_30.ttl",
        ontology_uri="source/dbpedia_ontology.owl",
    )

    # run_experiment(
    #     dataset_name="EnDe-Lite1000",
    #     dataset_uri="source/Datasets/EnDe-Lite1000(without_Ontology).ttl",
    #     shapes_graph_uri="source/ShapesGraphs/Shape_30.ttl",
    #     ontology_uri="source/dbpedia_ontology.owl",
    # )
