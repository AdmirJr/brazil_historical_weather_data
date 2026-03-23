import subprocess
from datetime import datetime
from pathlib import Path

# 1. Gerar a lista de URLs e caminhos de saída
year_list = range(2000, datetime.now().year+1)
input_file_path = "data/raw/download_list.txt"

with open(input_file_path, "w") as f:
    for y in year_list:
        if Path(f"data/raw/{y}_data.zip").exists():
            continue
        else:
            url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{y}.zip"
            f.write(f"{url}\n")
            f.write(f"  out={y}_data.zip\n")
            f.write("  dir=data/raw\n")

bash_command = [
    "aria2c", 
    "-i", input_file_path, 
    "-j", "3", 
    "-x", "5", 
    "-s", "5", 
    "--continue=true"
]

subprocess.run(bash_command)