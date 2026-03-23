from datetime import datetime

data_years = list(range(2000, datetime.now().year+1))

rule all:
    input:
        expand("data/raw/{year}_data.zip", year=data_years)

rule download_all:
    output:
        zips = expand("data/raw/{year}_data.zip", year=data_years),
        url_file = "data/raw/urls.txt"
    run:
        with open(output.url_file, "w") as f:
            for y in data_years:
                f.write(f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{y}.zip\n")
                f.write(f"  out={y}_data.zip\n")
        
        shell("aria2c -i {output.url_file} -j 3 -x 5 -d data/raw/ --allow-overwrite=true")