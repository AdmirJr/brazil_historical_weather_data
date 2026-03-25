import polars as pl
from pathlib import Path
from datetime import datetime

input_dir = Path(snakemake.input[0])
year = snakemake.wildcards.year

# input_dir = "data/raw/2005"
# year = "2005"

syno_map = {
    "date": ["Data", "DATA", "Data (YYYY-MM-DD)", "Data (ano-mes-dia)", "DATA (YYYY-MM-DD)"],
    "fund_date": ["DATA DE FUNDACAO", "DATA DE FUNDACAO (YYYY-MM-DD)", "DATA DE FUNDAÇÃO (YYYY-MM-DD)", "DATA DE FUNDAC?O"],
    "hour": ["Hora (UTC)", "HORA (UTC)", "Hora UTC", "HORA"],
    "precipitation (mm)": ["PRECIPITAÇÃO TOTAL, HORÁRIO (mm)", "PRECIPITACAO TOTAL, HORARIO (mm)", "Chuva (mm)"],
    "air_temperature (°C)": ["TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)", "TEMPERATURA DO AR - BULBO SECO, HORARIA (C)"],
    "atm_pressure (mB)": ["PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)", "PRESSAO ATMOSFERICA (mB)"],
    "atm_pressure_max_last_hour (mB)": ["PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)"],
    "atm_pressure_min_last_hour (mB)": ["PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)"],
    "wind_max_burst (m/s)": ["VENTO, RAJADA MAXIMA (m/s)"],
    "wind_velocity (m/s)": ["VENTO, VELOCIDADE HORARIA (m/s)"],
    "latitude": ["LATITUDE"],
    "longitude": ["LONGITUDE"],
    "global_radiation (kj/m2)": ["RADIACAO GLOBAL (KJ/m²)", "RADIACAO GLOBAL (Kj/m²)"],
    "region": ["REGIAO", "REGIÃO", "REGI?O"],
    "station": ["ESTACAO", "ESTAÇÃO", "ESTAC?O"],
    "uf": ["UF"],
    "code (wmo)": ["CODIGO (WMO)"],
    "altitude": ["ALTITUDE"],
    "air_temperature_max_last_hour (°C)": ["TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)"],
    "air_temperature_min_last_hour (°C)": ["TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)"],
    "dew_temperature (°C)": ["TEMPERATURA DO PONTO DE ORVALHO (°C)"],
    "dew_temperature_max_last_hour (°C)": ["TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)"],
    "dew_temperature_min_last_hour (°C)": ["TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)"],
    "air_humidity_max_last_hour (%)": ["UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)"],
    "air_humidity_min_last_hour (%)": ["UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)"],
    "air_humidity (%)": ["UMIDADE RELATIVA DO AR, HORARIA (%)"],
    "wind_clockwise_direction (°)": ["VENTO, DIREÇÃO HORARIA (gr) (° (gr))"]
}

def fix_col_names(bad_name, syno_map):
    for good_name, variations in syno_map.items():
        if bad_name in variations:
            return good_name
    return bad_name

#Path("data/processed_01/").mkdir(parents=True, exist_ok=True)

if not Path(f"data/processed_01/{year}_data.parquet").exists():
    print(f"Processing data for {year}...")

    dir_path = Path(f"data/raw/{year}")
    files = list(dir_path.glob("*.CSV"))
    
    df_year = []
    for f in files:
        df_year_region = pl.read_csv(
            f,
            separator = ";",
            skip_rows= 8,
            encoding = "latin-1",
            decimal_comma = True,
            infer_schema = False,
            truncate_ragged_lines = True
        )

        df_year_region = df_year_region[:, :-1]


        metadata = {}

        with open(f, 'r', encoding='latin-1') as f:
            for i in range(8):
                line = f.readline().strip()

                key, value = line.split(":;", 1)
                metadata[key.strip()] = value.strip()

        df_year_region = df_year_region.with_columns([
            pl.lit(value).alias(key) for key, value in metadata.items()
        ])

        dict_rename = {
            col: fix_col_names(col, syno_map) 
            for col in df_year_region.columns
        }
    
        df_year_region = df_year_region.rename(dict_rename)

        df_year_region.drop(["fund_date"])

        df_year.append(df_year_region)

    df_year = pl.concat(df_year, how='vertical_relaxed')

    col_float64 = [
        "precipitation (mm)",
        "atm_pressure (mB)",
        "atm_pressure_max_last_hour (mB)",
        "atm_pressure_min_last_hour (mB)",
        "global_radiation (kj/m2)",
        "wind_max_burst (m/s)",
        "wind_velocity (m/s)",
        "latitude",
        "longitude",
        "altitude"
    ]

    col_float32 = [
        "air_temperature_max_last_hour (°C)",
        "air_temperature_min_last_hour (°C)",
        "dew_temperature (°C)",
        "dew_temperature_max_last_hour (°C)",
        "dew_temperature_min_last_hour (°C)",
        "air_humidity_max_last_hour (%)",
        "air_humidity_min_last_hour (%)",
        "air_humidity (%)",
        "wind_clockwise_direction (°)"
    ]

    col_categorical = [
        "region",
        "uf",
        "station",
        "code (wmo)"
    ]

    df_year = df_year.with_columns([
        pl.col("date")
            .str.replace_all("/","-")
            .str.to_date(format="%Y-%m-%d"),
        pl.col("hour")
            .str.replace(" UTC","")
            .str.replace(":","")
            .str.to_time(format="%H%M"),
        pl.col(col_float64)
            .str.replace(",",".")
            .str.replace("-9999",None)
            .cast(pl.Float64, strict=False),
        pl.col(col_float32)
            .str.replace(",",".")
            .str.replace("-9999",None)
            .cast(pl.Float32, strict=False),
        pl.col(col_categorical)
            .cast(pl.Categorical)
    ])

    df_year.write_parquet("data/processed_01/"+f"{year}_data.parquet")