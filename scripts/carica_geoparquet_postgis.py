# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "geopandas>=1.0.0",
#     "pyarrow>=14.0",
#     "pyogrio>=0.7.0",
#     "sqlalchemy>=2.0",
#     "psycopg2-binary",
# ]
# ///
import os

import geopandas as gpd
from sqlalchemy import create_engine

HOST     = "pg.coseerobe.it"
PORT     = 5432
DB       = "anncsu-indirizzi"
USER     = os.environ["PG_USERNAME"]
PASSWORD = os.environ["PG_PASSWORD"]
SCHEMA   = "public"
TABELLA  = "anncsu-indirizzi-slim"
FILE     = "data/anncsu-indirizzi-slim.parquet"
CHUNK    = 100_000

engine = create_engine(
    f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
)

print("Lettura GeoParquet...")
gdf = gpd.read_parquet(FILE)
gdf = gdf.set_crs("EPSG:4326", allow_override=True)
print(f"  → {len(gdf)} record, CRS: {gdf.crs}")

totale = len(gdf)
for i, start in enumerate(range(0, totale, CHUNK)):
    chunk = gdf.iloc[start:start + CHUNK]
    modo = "replace" if i == 0 else "append"
    chunk.to_postgis(
        name=TABELLA,
        con=engine,
        schema=SCHEMA,
        if_exists=modo,
        index=False,
    )
    print(f"  → chunk {i+1}: {min(start + CHUNK, totale)}/{totale} record caricati")

print(f"Fatto! Tabella '{SCHEMA}.{TABELLA}' caricata su {HOST}/{DB}")
