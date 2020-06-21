"""
- population
- area
- density
- ages approximation
"""

import json
import os
from io import StringIO

import psycopg2

from geo_pop_data import r1, r2, r3, r4, r5, r6, r7


POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_PORT = os.environ['POSTGRES_PORT']
POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_CONNECTON_STRING = f'host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD}'


def parse_stat(name: str, data: str):
    result = []
    for l in data.strip().splitlines():
        if l.startswith(' '):
            continue
        l = l.strip()
        if l.startswith('Итого'):
            continue

        s_age, *s_parts = l.split()
        age = int(s_age) if s_age != '85+' else 85
        if name == 'Минск':
            assert len(s_parts) == 12, f'{name} - {len(s_parts)} - {l}'
            parts = [int(s_parts[2 * i] + s_parts[2 * i + 1]) for i in range(6)] + [0, 0, 0]
        else:
            if len(s_parts) != 18:
                len_prev_part = 1
                for i, part in reversed(list(enumerate(s_parts))):
                    len_part = len(part)
                    if len_prev_part == len_part == 3:
                        s_parts.insert(i + 1, '0')
                    len_prev_part = len_part
            assert len(s_parts) == 18, f'{name} - {len(s_parts)} - {l}'
            parts = [int(s_parts[2 * i] + s_parts[2 * i + 1]) for i in range(9)]
        result.append([age, parts])
        print(name, age, parts)
    return result


def approx_stat(population, town, stat):
    data = []
    sum = 0
    if town:
        for age, (_, _, _, total, men, woman, _, _, _) in stat:
            data.append((age, total, men, woman))
            sum += total
    else:
        for age, (_, _, _, _, _, _, total, men, woman) in stat:
            data.append((age, total, men, woman))
            sum += total
    result_list = []
    result_dict = {}
    for age, total, men, woman in data:
        pop_total = int(total * population / sum)
        pop_men = int(men * population / sum)
        pop_woman = int(woman * population / sum)
        result_dict[f'population_{age}'] = pop_total
        result_dict[f'population_m_{age}'] = pop_men
        result_dict[f'population_w_{age}'] = pop_woman
        result_list.append((age, pop_total, pop_men, pop_woman))
    return result_list, result_dict


stats = {}
selects = []
for m in [r1, r2, r3, r4, r5, r6, r7]:
    result = parse_stat(m.NAME, m.DATA)
    stats[m.OSM_ID] = result
    selects.append(f"""(
        SELECT p.osm_id, {m.OSM_ID} AS region, p.tags->'name:be' AS name_be, p.tags->'name:ru' AS name_ru, (p.tags->'population')::integer AS population, ST_MakeValid(p.way) AS geom 
        FROM planet_osm_polygon p
        INNER JOIN planet_osm_polygon b ON ST_Contains(b.way, p.way)
        WHERE b.osm_id = {m.OSM_ID} 
        AND (
            p.tags->'admin_level' IN ('6')
            OR p.tags->'place' IN ('city', 'town')
            OR (p.tags->'place' = 'village' AND p.tags->'name:prefix' IN ('городской посёлок', 'рабочий посёлок', 'курортный посёлок'))
        ) 
        ORDER BY (p.tags->'population')::integer DESC
    )""")


conn = psycopg2.connect(POSTGRES_CONNECTON_STRING)
cur = conn.cursor()
cur.execute('BEGIN')
cur.execute("""
    CREATE TABLE pop_places AS
    {}
""".format('\nUNION\n'.join(selects)))
cur.execute("""UPDATE pop_places SET population = 11445 WHERE osm_id = -5966883""")  # Калодзішчы
cur.execute("""UPDATE pop_places SET population = 7909 WHERE osm_id = -67703""")  # Ждановічы
cur.execute("""UPDATE pop_places SET population = 7308 WHERE osm_id = -7547117""")  # Гатава
print('selected')
cur.execute("""
    CREATE TABLE pop_places_flatten AS
    
    SELECT p1.osm_id, p1.region, 0 AS town, p1.name_be, p1.name_ru, p1.population - SUM(p2.population) AS population, ST_Difference(p1.geom, ST_Union(p2.geom)) AS geom
    FROM pop_places p1 
    INNER JOIN pop_places p2 ON p1.osm_id != p2.osm_id AND ST_Contains(p1.geom, p2.geom)
    GROUP BY p1.osm_id, p1.region, p1.name_be, p1.name_ru, p1.population, p1.geom
    
    UNION 
    
    SELECT p1.osm_id, p1.region, 1 AS town, p1.name_be, p1.name_ru, p1.population, p1.geom
    FROM pop_places p1 
    LEFT JOIN pop_places p2 ON p1.osm_id != p2.osm_id AND ST_Contains(p1.geom, p2.geom)
    WHERE p2.osm_id IS NULL
    GROUP BY p1.osm_id, p1.region, p1.name_be, p1.name_ru, p1.population, p1.geom
""")
print('flattened')
cur.execute("""
    CREATE TABLE pop_places_simplified AS
    SELECT p.osm_id, p.region, p.town, p.name_be, p.name_ru, p.population, ST_Area(p.geom::geography) / 1000 / 1000 AS area, ST_MakeValid(s.geom) AS geom
    FROM pop_places_flatten p
    INNER JOIN simplifyLayerPreserveTopology('', 'pop_places_flatten', 'osm_id', 'geom', 0.001) AS s (osm_id bigint, geom geometry) ON p.osm_id = s.osm_id
    ORDER BY p.population DESC
""")
print('simplified')
out = StringIO()
cur.copy_expert("""
    COPY (SELECT name_be, name_ru, population, area, osm_id, region, town, ST_AsGeoJSON(geom) FROM pop_places_simplified ORDER BY population DESC) TO STDOUT
""", out)
cur.execute('ROLLBACK')
cur.close()
conn.close()

features = []
for line in out.getvalue().strip().splitlines():
    name_be, name_ru, population_s, area_s, osm_id_s, region_s, town_s, geom_s = line.strip().split('\t')
    if name_be == '\\N':
        continue
    population = int(population_s)
    area = float(area_s)
    region = int(region_s)
    osm_id = int(osm_id_s)
    town = bool(int(town_s))
    geom = json.loads(geom_s)
    print(population, osm_id, name_be, name_ru, area, region, town_s)
    approx_list, approx_dict = approx_stat(population, town, stats[region])
    population_electoral = sum(total for age, total, men, woman in approx_list if 18 <= age)
    population_18_25 = sum(total for age, total, men, woman in approx_list if 18 <= age < 26)
    population_26_39 = sum(total for age, total, men, woman in approx_list if 26 <= age < 39)
    population_40_60 = sum(total for age, total, men, woman in approx_list if 40 <= age < 60)
    population_61_85 = sum(total for age, total, men, woman in approx_list if 61 <= age)
    features.append({
        'type': 'Feature',
        'geometry': geom,
        'properties': {
            'name_be': name_be,
            'name_ru': name_ru,
            'population': population,
            'population_electoral': population_electoral,
            'population_18_25': population_18_25,
            'population_26_39': population_26_39,
            'population_40_60': population_40_60,
            'population_61_85': population_61_85,
            'area': round(area, 2),
            'density': round(population / area, 2),
            'density_electoral': round(population_electoral / area, 2),
            'density_18_25': round(population_18_25 / area, 2),
            'density_26_39': round(population_26_39 / area, 2),
            'density_40_60': round(population_40_60 / area, 2),
            'density_61_85': round(population_61_85 / area, 2),
            # **approx_dict,
        }
    })
with open('geo_pop.geojson', 'w') as h:
    json.dump({'type': 'FeatureCollection', 'features': features}, h, indent=2, ensure_ascii=False)
