from rdflib import Graph, URIRef, Literal
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.namespace import OWL, RDF, RDFS, SH, XSD
import random
import requests
from collections import deque
import argparse
import pickle
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os

# Parse command line arguments
parser = argparse.ArgumentParser(description='Extract multi-size subgraphs incrementally from YAGO knowledge graph (Local version)')
parser.add_argument('--small', type=int, default=10,
                    help='Small graph: number of entities per class (default: 10)')
parser.add_argument('--medium', type=int, default=30,
                    help='Medium graph: total number of entities per class (default: 30)')
parser.add_argument('--large', type=int, default=60,
                    help='Large graph: total number of entities per class (default: 60)')
parser.add_argument('--max-depth', type=int, default=2,
                    help='Maximum depth for BFS expansion (default: 2)')
parser.add_argument('--output-prefix', type=str, default='yago_local',
                    help='Output file name prefix (default: yago_local)')
parser.add_argument('--output-dir', type=str, default='./output',
                    help='Output directory for generated graphs (default: ./output)')
parser.add_argument('--shacl-file', type=str, 
                    default='/Users/kejin/Developer/DE-TUM/simshapes/data/raw/YAGO-4_SHACL.ttl',
                    help='SHACL shape file path (default: /Users/kejin/Developer/DE-TUM/simshapes/data/raw/YAGO-4_SHACL.ttl)')
parser.add_argument('--endpoint', type=str, 
                    default='qlever',
                    choices=['qlever', 'official'],
                    help='SPARQL endpoint to use: qlever (faster) or official (default: qlever)')
parser.add_argument('--timeout', type=int, default=60,
                    help='SPARQL query timeout in seconds (default: 60)')
parser.add_argument('--max-retries', type=int, default=3,
                    help='Maximum number of retries for failed queries (default: 3)')
parser.add_argument('--workers', type=int, default=2,
                    help='Number of parallel workers for processing classes (default: 2)')
parser.add_argument('--resume-from', type=str, default=None,
                    choices=['small', 'medium'],
                    help='Resume from a previous size (skip small/medium and continue from that point)')
args = parser.parse_args()

# Load SHACL shape graph
SCHEMA = URIRef("http://schema.org/")

ENDPOINTS = {
    # keep keys so the CLI doesn't break, but both point to DBpedia now
    'qlever': 'https://dbpedia.org/sparql',
    'official': 'https://dbpedia.org/sparql',
}
YAGO_SPARQL_ENDPOINT = ENDPOINTS[args.endpoint]   # (name unchanged, but now DBpedia)
print(f"Using {args.endpoint.upper()} endpoint: {YAGO_SPARQL_ENDPOINT}")
 
XSD_TYPES = {
    "http://www.w3.org/2001/XMLSchema#dateTime": XSD.dateTime,
    "http://www.w3.org/2001/XMLSchema#integer": XSD.integer,
    "http://www.w3.org/2001/XMLSchema#int": XSD.int,
    "http://www.w3.org/2001/XMLSchema#float": XSD.float,
    "http://www.w3.org/2001/XMLSchema#double": XSD.double,
    "http://www.w3.org/2001/XMLSchema#boolean": XSD.boolean,
    "http://www.w3.org/2001/XMLSchema#string": XSD.string,
}

# 1. Parse SHACL shape graph and extract targetClass
print(f"Loading SHACL file: {args.shacl_file}")
shacl_graph = Graph()

# Check file format and load accordingly
if args.shacl_file.endswith('.pkl'):
    # Load pickle file
    with open(args.shacl_file, 'rb') as f:
        shacl_graph = pickle.load(f)
else:
    # Load turtle or other RDF format
    shacl_graph.parse(args.shacl_file, format="turtle")

# Extract all sh:targetClass
target_classes = set()

for cls in shacl_graph.objects(None, SH.targetClass):
    target_classes.add(cls)

print(f"Extracted {len(target_classes)} target classes:", target_classes)

# Initialize SPARQL client
sparql = SPARQLWrapper(YAGO_SPARQL_ENDPOINT)
sparql.setReturnFormat(JSON)
sparql.setTimeout(args.timeout)
sparql.setRequestMethod('POST')  # Use POST to avoid redirects

# Thread-local storage for SPARQL clients
thread_local = threading.local()

def get_sparql_client():
    """Get thread-local SPARQL client"""
    if not hasattr(thread_local, 'sparql'):
        thread_local.sparql = SPARQLWrapper(YAGO_SPARQL_ENDPOINT)
        thread_local.sparql.setReturnFormat(JSON)
        thread_local.sparql.setTimeout(args.timeout)
        # Handle HTTP redirects
        thread_local.sparql.addDefaultGraph = lambda x: None  # Disable for QLever
        thread_local.sparql.setRequestMethod('POST')  # Use POST to avoid redirects
    return thread_local.sparql

# Query with retry mechanism
def query_with_retry(sparql_query, max_retries=None):
    """
    Execute SPARQL query with retry mechanism
    :param sparql_query: SPARQL query string
    :param max_retries: Maximum retry attempts (uses args.max_retries if None)
    :return: Query results or None on failure
    """
    if max_retries is None:
        max_retries = args.max_retries
    
    sparql_client = get_sparql_client()
    
    for attempt in range(max_retries):
        try:
            sparql_client.setQuery(sparql_query)
            results = sparql_client.query().convert()
            return results
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                print(f"    ⚠️  Query failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s... Error: {e}")
                time.sleep(wait_time)
            else:
                print(f"    ❌ Query failed after {max_retries} attempts: {e}")
                return None
    return None

# Query YAGO via SPARQL to get sample entities of a targetClass
def get_sample_entities(target_class, num_samples, offset=0):
    """
    Get entities of specified class
    :param target_class: Target class
    :param num_samples: Number of entities to retrieve
    :param offset: Skip first N results (for incremental queries)
    """
    query = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX schema: <http://schema.org/>
    
    SELECT DISTINCT ?entity WHERE {{
        ?entity rdf:type <{target_class}> .
    }} LIMIT {num_samples} OFFSET {offset}
    """
    
    results = query_with_retry(query)
    if results:
        entities = [result["entity"]["value"] for result in results["results"]["bindings"]]
        return entities
    else:
        print(f"    ⚠️  Failed to retrieve entities for {target_class}, skipping...")
        return []

# Cache queried entities to avoid repeated queries (thread-safe)
# Use LRU-style cache with size limit to prevent memory overflow
entity_cache = {}
cache_lock = threading.Lock()
MAX_CACHE_SIZE = 50000  # Limit cache to 50K entities to prevent OOM

OWL_EQ = "http://www.w3.org/2002/07/owl#equivalentClass"
OWL_SAME = "http://www.w3.org/2002/07/owl#sameAs"
RDFS_SUB = "http://www.w3.org/2000/01/rdf-schema#subClassOf"

DBO = "http://dbpedia.org/ontology/"

def fetch_eq_same(classes):
    vals = " ".join(f"<{c}>" for c in classes)
    q = f"""
    SELECT DISTINCT ?x WHERE {{
      VALUES ?c {{ {vals} }}
      {{
        ?c <{OWL_EQ}> ?x .
      }} UNION {{
        ?x <{OWL_EQ}> ?c .
      }} UNION {{
        ?c <{OWL_SAME}> ?x .
      }} UNION {{
        ?x <{OWL_SAME}> ?c .
      }}
      FILTER(isIRI(?x))
      FILTER(STRSTARTS(STR(?x), "{DBO}"))   # dbo-only to avoid multilingual explosion
    }}
    """
    res = query_with_retry(q)
    if not res: return set()
    return {b["x"]["value"] for b in res["results"]["bindings"]}

def fetch_subclasses(classes):
    vals = " ".join(f"<{c}>" for c in classes)
    q = f"""
    SELECT DISTINCT ?sub WHERE {{
      VALUES ?c {{ {vals} }}
      ?sub <{RDFS_SUB}> ?c .
      FILTER(isIRI(?sub))
      FILTER(STRSTARTS(STR(?sub), "{DBO}"))
    }}
    """
    res = query_with_retry(q)
    if not res: return set()
    return {b["sub"]["value"] for b in res["results"]["bindings"]}

def compute_closure(seed_class: str, max_layers: int = 2) -> list[str]:
    seen = {seed_class}
    frontier = {seed_class}
    for _ in range(max_layers):
        eq = fetch_eq_same(frontier)
        sub = fetch_subclasses(frontier)
        new = (eq | sub) - seen
        if not new:
            break
        seen |= new
        frontier = new
    return sorted(seen)

# Query neighbors of an entity via SPARQL
def get_neighbors(entity):
    # Check cache (thread-safe)
    with cache_lock:
        if entity in entity_cache:
            return entity_cache[entity]
    
    query = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX schema: <http://schema.org/>
        SELECT ?p ?o WHERE {{
        <{entity}> ?p ?o .
        FILTER(?p NOT IN (
            dbo:wikiPageWikiLink,
            dbo:wikiPageExternalLink,
            dbo:wikiPageRedirects,
            dbo:wikiPageDisambiguates,
            dbo:wikiPageID,
            dbo:wikiPageRevisionID
        ))
        }}  LIMIT 200
    """
    
    results = query_with_retry(query)
    if results:
        triples = []
        for result in results["results"]["bindings"]:
            s = URIRef(entity)
            p = URIRef(result["p"]["value"])
            o_value = result["o"]["value"]
            o_rdflib = None

            # Determine the type of `o`
            if "type" in result["o"] and result["o"]["type"] == "uri":
                o_rdflib = URIRef(o_value)  # Handle URI
            elif "datatype" in result["o"]:
                # Handle Literal with datatype (fixed: should be in result["o"])
                datatype_uri = result["o"]["datatype"]
                xsd_type = XSD_TYPES.get(datatype_uri, URIRef(datatype_uri))  # Get corresponding rdflib XSD type
                o_rdflib = Literal(o_value, datatype=xsd_type)
            elif "xml:lang" in result["o"]:
                # Handle Literal with language tag
                o_rdflib = Literal(o_value, lang=result["o"]["xml:lang"])
            else:
                # Handle default string Literal
                o_rdflib = Literal(o_value)

            # Ensure `o_rdflib` is correctly stored in RDF graph
            if o_rdflib:
                triples.append((s, p, o_rdflib))
        
        # Store in cache (thread-safe) with size limit
        with cache_lock:
            # If cache is too large, clear oldest 25% entries
            if len(entity_cache) >= MAX_CACHE_SIZE:
                keys_to_remove = list(entity_cache.keys())[:MAX_CACHE_SIZE // 4]
                for key in keys_to_remove:
                    del entity_cache[key]
            entity_cache[entity] = triples
        return triples
    else:
        # Query failed after all retries (thread-safe)
        with cache_lock:
            if len(entity_cache) < MAX_CACHE_SIZE:
                entity_cache[entity] = []  # Cache failed result to avoid retry
        return []

# Use BFS to extract subgraph with depth of 2 (streaming version)
def bfs_extract_subgraph_streaming(seed_entities, output_file, max_depth=2):
    """
    Stream triples directly to file instead of holding in memory
    :param seed_entities: Starting entities
    :param output_file: File path to write triples
    :param max_depth: Maximum BFS depth
    :return: Number of triples written
    """
    visited = set()
    queue = deque([(entity, 0) for entity in seed_entities])
    
    total_entities = len(seed_entities)
    processed = 0
    triple_count = 0
    
    # Open file in append mode for streaming writes
    with open(output_file, 'a', encoding='utf-8') as f:
        while queue:
            entity, depth = queue.popleft()
            if entity in visited or depth > max_depth:
                continue
            visited.add(entity)
            
            if depth == 0:
                processed += 1
                if processed % 5 == 0:
                    print(f"  Progress: {processed}/{total_entities} seed entities, {triple_count} triples written")
            
            # Get neighbors and write immediately
            neighbors = get_neighbors(entity)
            
            for s, p, o in neighbors:
                # Write triple in N-Triples format (simpler than TTL, one line per triple)
                if isinstance(o, URIRef):
                    line = f"<{s}> <{p}> <{o}> .\n"
                elif isinstance(o, Literal):
                    if o.datatype:
                        line = f'<{s}> <{p}> "{o}"^^<{o.datatype}> .\n'
                    elif o.language:
                        line = f'<{s}> <{p}> "{o}"@{o.language} .\n'
                    else:
                        # Escape quotes in literal value
                        escaped_value = str(o).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                        line = f'<{s}> <{p}> "{escaped_value}" .\n'
                else:
                    continue
                
                f.write(line)
                triple_count += 1
                
                # Queue YAGO entities for expansion
                if isinstance(o, URIRef):
                    u = str(o)
                    if u.startswith("http://dbpedia.org/resource/") or u.startswith("https://dbpedia.org/resource/"):
                        queue.append((o, depth + 1))

    
    print(f"  Visited {len(visited)} entities, wrote {triple_count} triples to {output_file}")
    return triple_count

# Process a single target class (streaming version)
def process_target_class_streaming(
    target_class,
    target_total,
    downloaded_count,
    max_depth,
    worker_id,
    output_file,
    closure_layers=2,
):
    """
    Process one target class:
      - compute closure(target_class) via eq/sameAs + subclasses (downward)
      - sample instances from all classes in the closure
      - BFS-expand from sampled seeds and stream to file
    """
    increment = target_total - downloaded_count
    if increment <= 0:
        print(f"[Worker {worker_id}] {target_class}: Target reached, skipping")
        return 0, 0

    print(f"[Worker {worker_id}] Processing class: {target_class}")
    print(f"[Worker {worker_id}]   Already have {downloaded_count}, need {increment} more")

    # 1) compute closure classes (dbo-only restriction happens in your compute_closure())
    closure = compute_closure(str(target_class), max_layers=closure_layers)
    if not closure:
        print(f"[Worker {worker_id}]   ⚠️ closure is empty, skipping")
        return 0, 0

    # 2) distribute increment across closure classes
    per_c = max(1, increment // len(closure))

    new_entities = []
    for c in closure:
        # offset=0 is fine here; we just want enough seeds quickly
        ents = get_sample_entities(c, per_c, offset=0)
        new_entities.extend(ents)

        if len(new_entities) >= increment:
            break

    # dedupe + cap
    new_entities = list(dict.fromkeys(new_entities))[:increment]

    print(
        f"[Worker {worker_id}]   closure_size={len(closure)} closure_layers={closure_layers} "
        f"per_c={per_c} seeds={len(new_entities)}"
    )

    if not new_entities:
        print(f"[Worker {worker_id}]   ⚠️ No instances retrieved from closure")
        return 0, 0

    triple_count = bfs_extract_subgraph_streaming(new_entities, output_file, max_depth=max_depth)
    return triple_count, len(new_entities)

# Initialize final subgraph
print(f"\n{'='*60}")
print(f"Configuration (Local Test Version):")
print(f"{'='*60}")
print(f"  Endpoint: {args.endpoint.upper()} ({YAGO_SPARQL_ENDPOINT})")
print(f"  Timeout: {args.timeout}s")
print(f"  Max retries: {args.max_retries}")
print(f"  Workers: {args.workers}")
print(f"  Small graph: {args.small} entities per class")
print(f"  Medium graph: {args.medium} entities per class (increment {args.medium - args.small})")
print(f"  Large graph: {args.large} entities per class (increment {args.large - args.medium})")
print(f"  Output prefix: {args.output_prefix}")
print(f"  Output directory: {args.output_dir}")
print(f"  Output format: N-Triples (.nt)")
print(f"\nStarting to process {len(target_classes)} target classes...\n")

# Track downloaded entity count per class
downloaded_per_class = {}
downloaded_lock = threading.Lock()

# Create output directory if it doesn't exist
os.makedirs(args.output_dir, exist_ok=True)
print(f"Output directory created: {args.output_dir}\n")

# Define three size configurations (N-Triples format)
sizes = [
    {"name": "small", "total": args.small, "output": os.path.join(args.output_dir, f"{args.output_prefix}_small.nt")},
    {"name": "medium", "total": args.medium, "output": os.path.join(args.output_dir, f"{args.output_prefix}_medium.nt")},
    {"name": "large", "total": args.large, "output": os.path.join(args.output_dir, f"{args.output_prefix}_large.nt")}
]

# If resuming from a previous size, skip completed sizes and initialize downloaded count
if args.resume_from:
    print(f"\n{'='*60}")
    print(f"RESUME MODE: Skipping completed sizes up to {args.resume_from.upper()}")
    print(f"{'='*60}\n")
    
    # Find the index to resume from
    resume_index = next(i for i, s in enumerate(sizes) if s["name"] == args.resume_from)
    
    # Initialize downloaded_per_class with the count from resume point
    resume_total = sizes[resume_index]["total"]
    for target_class in target_classes:
        downloaded_per_class[target_class] = resume_total
    
    print(f"Initialized downloaded count: {resume_total} entities per class")
    print(f"Will generate sizes: {[s['name'] for s in sizes[resume_index+1:]]}\n")
    
    # Only process sizes after the resume point
    sizes = sizes[resume_index + 1:]

for size_config in sizes:
    size_name = size_config["name"]
    target_total = size_config["total"]
    output_file = size_config["output"]
    
    print(f"\n{'='*60}")
    print(f"Starting to generate {size_name.upper()} size subgraph (target per class: {target_total} entities)")
    print(f"{'='*60}\n")
    
    # Prepare tasks for parallel processing
    target_classes_list = list(target_classes)
    
    # Each worker writes to its own temporary file
    worker_files = {}
    total_triples = 0
    
    # Process classes in parallel
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        future_to_class = {}
        for idx, target_class in enumerate(target_classes_list):
            with downloaded_lock:
                current_count = downloaded_per_class.get(target_class, 0)
            
            worker_id = idx % args.workers
            
            # Create worker-specific file
            worker_file = os.path.join(args.output_dir, f".{args.output_prefix}_{size_name}_worker_{worker_id}.nt.tmp")
            worker_files[worker_id] = worker_file
            closure_layers = 1 if size_name in ("small", "medium") else 2
            future = executor.submit(
                process_target_class_streaming,
                target_class,
                target_total,
                current_count,
                args.max_depth,
                worker_id,
                worker_file
            )
            future_to_class[future] = target_class
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_class):
            target_class = future_to_class[future]
            completed += 1
            
            try:
                triple_count, new_entities_count = future.result()
                total_triples += triple_count
                
                # Update downloaded count
                if new_entities_count > 0:
                    with downloaded_lock:
                        current = downloaded_per_class.get(target_class, 0)
                        downloaded_per_class[target_class] = current + new_entities_count
                
                print(f"Progress: {completed}/{len(target_classes_list)} classes completed\n")
                
            except Exception as e:
                print(f"❌ Error processing {target_class}: {e}\n")
    
    # Merge all worker files into final output
    print(f"\nMerging worker files into {size_name.upper()} size subgraph...")
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for worker_id, worker_file in worker_files.items():
            if os.path.exists(worker_file):
                print(f"  Merging worker {worker_id} file...")
                with open(worker_file, 'r', encoding='utf-8') as infile:
                    outfile.write(infile.read())
                # Delete temporary file
                os.remove(worker_file)
    
    print(f"✅ {size_name.upper()} size subgraph saved: {output_file}")
    print(f"   Total triples: {total_triples}")
    print(f"   Total queried entities: {len(entity_cache)}\n")

print(f"\n{'='*60}")
print(f"✅ All generation completed!")
print(f"{'='*60}")
print(f"Generated files:")
for size_config in sizes:
    print(f"  - {size_config['output']}")
print(f"\nYou can now inspect the generated graphs in: {args.output_dir}")
