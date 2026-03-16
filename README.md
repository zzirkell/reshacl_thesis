# Runner README

## What this runner does

`run.py` benchmarks SHACL validation on RDF datasets using the ReSHACL-based pipeline.

For each dataset, it:

1. loads the data graph
2. loads the ontology into the data graph
3. loads the shapes graph (`DBpedia_SHACL_selected30_no_shin.ttl` represents shapes without in singleton lists, use `DBpedia_SHACL_selected30.ttl` for the general pipeline usage)
4. builds the validation graph with the selected method
5. validates with `pyshacl`
6. stores runtime statistics, validation reports, and violation graphs

The script is currently configured to run three datasets in sequence:

- `small`
- `medium`
- `large`

These are started from the `if __name__ == "__main__":` block.  
The actual benchmark logic is inside `run_experiment(...)` and `benchmark_method(...)`.

---

## Current methods in the runner

At the moment, the active methods in `run_experiment(...)` are:

- `original` → `reshacl`
- `rdflib` → `engine_rdflib`
- `SPARQL` → `engine_sparql`
---

## How to run

Run the script from the project root:

```bash
python run.py
