import json

#& "C:\Users\mazek.ZZIRKELL\AppData\Local\Python\pythoncore-3.14-64\Scripts\scalene.exe" view "C:\Users\mazek.ZZIRKELL\reshacl_thesis\run_experiment.py.scalene.json"

with open("C:/Users/mazek.ZZIRKELL/reshacl_thesis/run_experiment.py.scalene.json") as f:
    data = json.load(f)

functions = data.get("functions", {})
for func_name, stats in functions.items():
    cpu_time = stats.get("cpu_time", 0)
    print(f"{func_name}: {cpu_time:.4f} seconds")
