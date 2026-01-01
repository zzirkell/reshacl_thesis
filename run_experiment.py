from importlib import reload
from rdflib import Graph, Namespace
from pyshacl import validate
import time
import sys
from prettytable import PrettyTable
import numpy as np
from reSHACL.re_shacl import merged_graph
from reSHACL.re_shacl_no_tc import merged_graph_no_tc
import os
import logging


time.sleep(10)  # wait 3 seconds before doing anything


DBO = Namespace("http://dbpedia.org/ontology/")
sys.path.insert(0, sys.path[0] + "/../")

if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding("utf-8")


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

def _ns_to_s(ns: int) -> float:
    return ns / 1_000_000_000.0


def _timing_to_pretty_kv(timing: dict) -> str:
    # pretty-print all numeric timings in seconds; keep other keys as-is
    parts = []
    for k in sorted(timing.keys()):
        v = timing[k]
        if isinstance(v, int) and k.endswith("_ns"):
            parts.append(f"{k}={_ns_to_s(v):.6f}s")
        else:
            parts.append(f"{k}={v}")
    return ", ".join(parts)

def run_reshacl(dataset_name, g, sg, inference_method):
    table = PrettyTable(['Method', 'Average validation time (s)', 'Standard deviation', 'Conform', '#Violation'])

    result_query = """
    SELECT ?v
    WHERE {
        ?s sh:result ?v
    }"""

    inter_time = []
    for n2 in range(0, 3):
        t3 = time.time()
        fused_graph1, same_dic1, shapes, timing = merged_graph(g, shacl_graph=sg, data_graph_format='turtle',
                                                       shacl_graph_format='turtle')
        t5 = time.time()
        timerss = t5-t3
        print(' #build: ', timerss)
        
        shapes.bind("dbo", DBO)
        conform, v_g, v_t = validate(fused_graph1, shacl_graph=shapes, inference=inference_method)
        t4 = time.time()
        print(' #valid: ', t4 - t5)

        inter_time.append(t4 - t3)

    mean_time = np.mean(inter_time)
    std = np.std(inter_time)

    result = v_g.query(result_query)

    print(f'[ReSHACL]=============================')

    print(' Average validation time: ', mean_time, 's')
    print(' Standard deviation: ', std, 's')
    print(' #Violation: ', len(result))
    

    # Saving the validation report graph
    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/violationGraph/")
    v_g.serialize(destination=f"Outputs/{dataset_name}/violationGraph/re-shacl_results.ttl")

    # Saving the validation report in txt
    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/validationReports/")
    file = open(f"Outputs/{dataset_name}/validationReports/re-shacl_results.txt", "w")
    file.write(v_t)
    file.close()

    table.add_row(["ReSHACL", mean_time, std, conform, len(result)])

    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/")
    file_table = open(f"Outputs/{dataset_name}/RunTimeResults.txt", "a+")
    file_table.write(str(table))
    file_table.close()

    print(table)

def run_reshacl_no_tc(dataset_name, g, sg, inference_method, ontology):
    table = PrettyTable(['Method', 'Average validation time (s)', 'Standard deviation', 'Conform', '#Violation'])

    result_query = """
    SELECT ?v
    WHERE {
        ?s sh:result ?v
    }"""

    inter_time = []
    for n2 in range(0, 3):
        t3 = time.time()
        fused_graph1, same_dic1, shapes, timing = merged_graph_no_tc(g, ontology, shacl_graph=sg, data_graph_format='turtle',
                                                       shacl_graph_format='turtle', ontology_format='xml')
        t5 = time.time()
        timerss = t5-t3
        
        print(' #build: ', timerss)
        shapes.bind("dbo", DBO)
        conform, v_g, v_t = validate(fused_graph1, shacl_graph=shapes, inference=inference_method)
        t4 = time.time()
        print(' #valid: ', t4 - t5)
        inter_time.append(t4 - t3)

    mean_time = np.mean(inter_time)
    std = np.std(inter_time)

    result = v_g.query(result_query)

    print(f'[ReSHACL-TCREASONING]=============================')

    print(' Average validation time: ', mean_time, 's')
    print(' Standard deviation: ', std, 's')
    print(' #Violation: ', len(result))
    

    # Saving the validation report graph
    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/violationGraph/")
    v_g.serialize(destination=f"Outputs/{dataset_name}/violationGraph/re-shacl_results.ttl")

    # Saving the validation report in txt
    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/validationReports/")
    file = open(f"Outputs/{dataset_name}/validationReports/re-shacl_results.txt", "w")
    file.write(v_t)
    file.close()

    #table.add_row(["ReSHACL", mean_time, std, conform, len(result)])

    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/")
    file_table = open(f"Outputs/{dataset_name}/RunTimeResults.txt", "a+")
    file_table.write(str(table))
    file_table.close()

    print(table)

def run_reshacl_nv(dataset_name, g, sg, inference_method):
    table = PrettyTable(['Method', 'Average validation time (s)', 'Standard deviation', 'Conform', '#Violation'])

    result_query = """
    SELECT ?v
    WHERE {
        ?s sh:result ?v
    }"""

    inter_time = []
    for n2 in range(0, 3):
        t3 = time.time()
        fused_graph1, same_dic1, shapes, timing = merged_graph(g, shacl_graph=sg, data_graph_format='turtle',
                                                       shacl_graph_format='turtle')
        t5 = time.time()
        timerss = t5-t3
        print(' #build: ', timerss)

    mean_time = np.mean(inter_time)
    std = np.std(inter_time)

    print(f'[ReSHACL]=============================')

    print(' Average validation time: ', mean_time, 's')
    print(' Standard deviation: ', std, 's')

    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/")
    file_table = open(f"Outputs/{dataset_name}/RunTimeResults.txt", "a+")
    file_table.write(str(table))
    file_table.close()

    print(table)

def run_reshacl_no_tc_nv(dataset_name, g, sg, inference_method, ontology):
    table = PrettyTable(['Method', 'Average validation time (s)', 'Standard deviation', 'Conform', '#Violation'])

    result_query = """
    SELECT ?v
    WHERE {
        ?s sh:result ?v
    }"""

    inter_time = []
    for n2 in range(0, 3):
        t3 = time.time()
        fused_graph1, same_dic1, shapes, timing = merged_graph_no_tc(g, ontology, shacl_graph=sg, data_graph_format='turtle',
                                                       shacl_graph_format='turtle', ontology_format='xml')
        t5 = time.time()
        timerss = t5-t3
    print(' #build: ', timerss)
    #table.add_row(["ReSHACL", mean_time, std, conform, len(result)])

    check_directory_exists_otherwise_create(f"Outputs/{dataset_name}/")
    file_table = open(f"Outputs/{dataset_name}/RunTimeResults.txt", "a+")
    file_table.write(str(table))
    file_table.close()

    print(table)

def run_experiment(dataset_name, dataset_uri, shapes_graph_uri, method='pyshacl', ontology=''):
    g = Graph()
    # Loading the data graph
    print("***** Loading the data graph *****")
    logging.getLogger('rdflib').setLevel(logging.ERROR)
    g.parse(dataset_uri)

    if ontology != '':
        # Importing Ontology into the data graph
        print("***** Loading the ontology *****")
        g.parse(ontology, format="xml")

    sg = Graph()
    # Loading the shapes graph
    print("***** Loading the shapes graph *****")
    sg.parse(shapes_graph_uri)
    sg.bind("dbo", DBO)

    # Preheating with 10 rounds
    i = 0
    for i in range(5):
         conform, v_g, v_t = validate(g, shacl_graph=sg, inference='none')

    print(f"***** START VALIDATION ON [{dataset_name}] *****")
    if method == 'reshacl':
        run_reshacl(dataset_name, g, sg, 'none')
    elif method == 'reshacl_no_tc':
        run_reshacl_no_tc(dataset_name, g, sg, 'none', ontology)

def run_experiment_nv(dataset_name, dataset_uri, shapes_graph_uri, method='pyshacl', ontology=''):
    g = Graph()
    # Loading the data graph
    print("***** Loading the data graph *****")
    logging.getLogger('rdflib').setLevel(logging.ERROR)
    g.parse(dataset_uri)

    if ontology != '':
        # Importing Ontology into the data graph
        print("***** Loading the ontology *****")
        g.parse(ontology, format="xml")

    sg = Graph()
    # Loading the shapes graph
    print("***** Loading the shapes graph *****")
    sg.parse(shapes_graph_uri)
    sg.bind("dbo", DBO)

    print(f"***** START BUILD ON [{dataset_name}] *****")
    if method == 'reshacl':
        run_reshacl_nv(dataset_name, g, sg, 'none')
    elif method == 'reshacl_no_tc':
        run_reshacl_no_tc_nv(dataset_name, g, sg, 'none', ontology)

# if __name__ == "__main__":
#     # run_experiment(dataset_name="EnDe-Lite100",
#     #                dataset_uri="reshacl_thesis/source/datasets/EnDe-Lite100(without_Ontology).ttl",
#     #                shapes_graph_uri="reshacl_thesis/source/shapesg/Shape_30.ttl",
#     #                method='reshacl_no_tc',
#     #                ontology="reshacl_thesis/source/datasets/dbpedia_ontology.owl")
#     # run_experiment("EnDe-Lite100",
#     #                "reshacl_thesis/source/datasets/EnDe-Lite100(without_Ontology).ttl",
#     #                "reshacl_thesis/source/shapesg/Shape_30.ttl",
#     #                method='reshacl',
#     #                ontology="reshacl_thesis/source/datasets/dbpedia_ontology.owl")
#     # run_experiment(dataset_name="EnDe-Lite1000",
#     #                dataset_uri="reshacl_thesis/source/datasets/EnDe-Lite1000(without_Ontology).ttl",
#     #                shapes_graph_uri="reshacl_thesis/source/shapesg/Shape_30.ttl",
#     #                method='reshacl_no_tc',
#     #                ontology="reshacl_thesis/source/datasets/dbpedia_ontology.owl")
#     # run_experiment("EnDe-Lite1000",
#     #                "reshacl_thesis/source/datasets/EnDe-Lite1000(without_Ontology).ttl",
#     #                "reshacl_thesis/source/shapesg/Shape_30.ttl",
#     #                method='reshacl',
#     #                ontology="reshacl_thesis/source/datasets/dbpedia_ontology.owl")
#     run_experiment_nv(dataset_name="EnDe-Lite50",
#                    dataset_uri="reshacl_thesis/source/datasets/EnDe-Lite100(without_Ontology).ttl",
#                    shapes_graph_uri="reshacl_thesis/source/shapesg/Shape_30.ttl",
#                    method='reshacl',
#                    ontology="reshacl_thesis/source/datasets/dbpedia_ontology.owl")
#     run_experiment_nv("EnDe-Lite50",
#                    "reshacl_thesis/source/datasets/EnDe-Lite100(without_Ontology).ttl",
#                    "reshacl_thesis/source/shapesg/Shape_30.ttl",
#                    method='reshacl',
#                    ontology="reshacl_thesis/source/datasets/dbpedia_ontology.owl")

