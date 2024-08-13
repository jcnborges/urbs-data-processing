### URBS DATA PROCESSING 

### Build docker-image

``` 
docker-compose build
```

### Download URBS Data
```

### download files trom UFPR portal

docker-compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-15" -fd folder -fl file

-fd: linhas, pontoslinha, veiculos
-fl: linhas.json.xz, pontosLinha.json.xz, veiculos.json.xz

## Examples

docker-compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-15" -fd linhas -fl linhas.json.xz

docker-compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-15" -fd pontoslinha -fl pontosLinha.json.xz

docker-compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-15" -fd veiculos -fl veiculos.json.xz


```
### Uncompress URBS Data
```

## uncompress urbs data 

docker-compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-15" -fd linhas -fl linhas.json.xz

-fd: linhas, pontoslinha, veiculos
-fl: linhas.json.xz, pontosLinha.json.xz, veiculos.json.xz

docker-compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-15" -fd linhas -fl linhas.json.xz

docker-compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-15" -fd pontoslinha -fl pontosLinha.json.xz

docker-compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-15" -fd veiculos -fl veiculos.json.xz

```

### Execute trusting processor
```
## process entire month data, prepare, deduplicate and clean for following processing pipelines.

docker-compose exec jupyterlab  python dataprocessing/job/trust_ingestion.py -d "2022-07"

```

### Execute refined processor 
```

### Execute refined processing for creating several enriched datasources.

docker-compose exec jupyterlab  python dataprocessing/job/refined_ingestion.py -ds "2019-05-03" -de "2019-05-03" -j line

-j [line,timetable,bus-stop, tracking]

docker-compose exec jupyterlab  python dataprocessing/job/refined_ingestion.py -ds "2019-05-03" -de "2019-05-03" -j line

docker-compose exec jupyterlab  python dataprocessing/job/refined_ingestion.py -ds "2019-05-03" -de "2019-05-03" -j timetable

docker-compose exec jupyterlab  python dataprocessing/job/refined_ingestion.py -ds "2019-05-03" -de "2019-05-03" -j bus-stop

docker-compose exec jupyterlab  python dataprocessing/job/refined_ingestion.py -ds "2019-05-03" -de "2019-05-03" -j tracking

```