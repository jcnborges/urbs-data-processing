### URBS DATA PROCESSING 

### Build docker-image

``` 
docker-compose build
```

### Download URBS Data
```

### download files trom UFPR portal

docker-compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-16" -fd folder -fl file

-fd: linhas, pontoslinha, veiculos
-fl: linhas.json.xz, pontosLinha.json.xz, veiculos.json.xz

## Examples

docker-compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-16" -fd linhas -fl linhas.json.xz

docker-compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-16" -fd pontoslinha -fl pontosLinha.json.xz

docker-compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-16" -fd veiculos -fl veiculos.json.xz


```
### Uncompress URBS Data
```

## uncompress urbs data 

docker-compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-16" -fd linhas -fl linhas.json.xz

-fd: linhas, pontoslinha, veiculos
-fl: linhas.json.xz, pontosLinha.json.xz, veiculos.json.xz

docker-compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-16" -fd linhas -fl linhas.json.xz

docker-compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-16" -fd pontoslinha -fl pontosLinha.json.xz

docker-compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-16" -fd veiculos -fl veiculos.json.xz

```

### Execute trusting processor
```
## process entire month data, prepare, deduplicate and clean for following processing pipelines.

docker-compose exec jupyterlab  python dataprocessing/job/trust_ingestion.py -d "2022-07"

```

### Execute refined processor 
```

### Execute refined processing for creating several enriched datasources.

docker-compose exec jupyterlab  python dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-16" -j line

-j [line, itinerary, tracking]

docker-compose exec jupyterlab  python dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-16" -j line

docker-compose exec jupyterlab  python dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-16" -j itinerary

docker-compose exec jupyterlab  python dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-16" -j tracking

```

### Load data into MySQL

```
docker-compose exec jupyterlab  python dataprocessing/job/mysql_loader.py -ds "2022-07-11"  -de "2022-07-16"

```