import polars as pl

data_path = "data/processed_01"
df_lazy = pl.scan_parquet(data_path)

metadata_columns = [
    'region',
    'uf',
    'station',
    'code (wmo)',
    'latitude',
    'longitude',
    'altitude'
]

metadata_table = (
    df_lazy
    .select(metadata_columns)
    .unique()
    .collect(engine="streaming") 
)

variables = [c for c in df_lazy.collect_schema().names() if c not in ["date", "hour", "latitude", "longitude", "altitude", "fund_date", "region", "uf", "station", "code (wmo)"]]

for i in range(0,metadata_table.shape[0]): 
    meta = metadata_table.row(i)
    reg, uf, est = meta[0], meta[1], meta[2]

    df_temp = df_lazy.filter(
        (pl.col("region") == reg) & 
        (pl.col("uf") == uf) & 
        (pl.col("station") == est)
    ).with_columns( # DEBUG REMOVE ME PLEASE
        pl.col("air_temperature (°C)") #
            .str.replace(",",".") #
            .str.replace("-9999",None) #
            .cast(pl.Float32, strict=False) #
    ).sort(
        ["date","hour"]
    ).select([
        pl.col("date")
        .filter(
            pl.col(c).is_not_null()
        )
        .last()
        .alias(f"lad_{c}")
        for c in variables
    ])



    

metadata_columns = [
    'region',
    'uf',
    'station',
    'code (wmo)',
    'latitude',
    'longitude',
    'altitude'
]

metadata_table.write_parquet("data/station_data.parquet", compression="snappy")
