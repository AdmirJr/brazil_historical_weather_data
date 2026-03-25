from datetime import datetime

data_years = list(range(2000, datetime.now().year+1))

rule all:
    input:
        expand("data/processed_01/{year}_data.parquet", year=data_years)
    
rule download:
    output:
        zips = temp(expand("data/raw/{year}_data.zip", year=data_years)),
        url_file = "data/raw/urls.txt"
    run:
        with open(output.url_file, "w") as f:
            for y in data_years:
                f.write(f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{y}.zip\n")
                f.write(f"  out={y}_data.zip\n")
        
        shell("aria2c -i {output.url_file} -j 3 -x 5 -d data/raw/ --allow-overwrite=true")

rule unzip:
    input:
        zip = "data/raw/{year}_data.zip"
    output:
        out_dir = directory("data/raw/{year}/")
    shell:
        """
        mkdir -p {output.out_dir}
        unzip -qq -o -j {input.zip} -d {output.out_dir}
        rm -f {input.zip}
        """

rule process_layer_01:
    input:
        "data/raw/{year}/"
    output:
        "data/processed_01/{year}_data.parquet"
    resources:
        mem_mb = 1500
    conda:
        "envs/processing.yaml"
    script:
        "scripts/processing_lyr_01.py"