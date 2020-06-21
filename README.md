# Approximate age-gender-geo population division in Belarus

## Demo

- https://tbicr.github.io/bel_pop/population.html
- https://tbicr.github.io/bel_pop/density.html

## Run

    docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgis/postgis:12-3.0-alpine
    wget https://download.geofabrik.de/europe/belarus-latest.osm.pbf
    PGPASSWORD=postgres osm2pgsql -H localhost -U postgres -d postgres \
        -l -j -G --hstore-add-index -C 10000 belarus-latest.osm.pbf
    pip install -r requirements.txt
    POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=postgres \
        POSTGRES_USER=postgres POSTGRES_PASSWORD=postgres python geo_pop.py