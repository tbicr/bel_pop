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

from geo_pop_data.detailed import get_info_normalized

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
        # print(name, age, parts)
    return result


def approx_stat0(populations, town, stat):
    p_lo_m, p_lo_w, p_md_m, p_md_w, p_hi_m, p_hi_w = populations
    if town:
        c_lo_m = sum(town_m for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if age <= 15)
        c_lo_w = sum(town_w for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if age <= 15)
        c_md_m = sum(town_m for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 16 <= age <= 60)
        c_md_w = sum(town_w for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 16 <= age <= 55)
        c_hi_m = sum(town_m for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 61 <= age)
        c_hi_w = sum(town_w for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 56 <= age)

        a_lo_m = {age: round(town_m * p_lo_m / c_lo_m) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if age <= 15}
        a_lo_w = {age: round(town_w * p_lo_w / c_lo_w) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if age <= 15}
        a_md_m = {age: round(town_m * p_md_m / c_md_m) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 16 <= age <= 60}
        a_md_w = {age: round(town_w * p_md_w / c_md_w) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 16 <= age <= 55}
        a_hi_m = {age: round(town_m * p_hi_m / c_hi_m) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 61 <= age}
        a_hi_w = {age: round(town_w * p_hi_w / c_hi_w) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 56 <= age}
    else:
        c_lo_m = sum(vil_m for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if age <= 15)
        c_lo_w = sum(vil_w for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if age <= 15)
        c_md_m = sum(vil_m for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 16 <= age <= 60)
        c_md_w = sum(vil_w for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 16 <= age <= 55)
        c_hi_m = sum(vil_m for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 61 <= age)
        c_hi_w = sum(vil_w for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 56 <= age)

        a_lo_m = {age: round(vil_m * p_lo_m / c_lo_m) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if age <= 15}
        a_lo_w = {age: round(vil_w * p_lo_w / c_lo_w) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if age <= 15}
        a_md_m = {age: round(vil_m * p_md_m / c_md_m) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 16 <= age <= 60}
        a_md_w = {age: round(vil_w * p_md_w / c_md_w) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 16 <= age <= 55}
        a_hi_m = {age: round(vil_m * p_hi_m / c_hi_m) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 61 <= age}
        a_hi_w = {age: round(vil_w * p_hi_w / c_hi_w) for age, (_, _, _, _, town_m, town_w, _, vil_m, vil_w) in stat if 56 <= age}

    a_m = {**a_lo_m, **a_md_m, **a_hi_m}
    a_w = {**a_lo_w, **a_md_w, **a_hi_w}
    data = {}
    for (age_m, pop_m), (age_w, pop_w) in zip(sorted(a_m.items()), sorted(a_w.items())):
        assert age_m == age_w
        data[age_m] = [pop_m, pop_w]
    return data


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


conn = psycopg2.connect(POSTGRES_CONNECTON_STRING)
cur = conn.cursor()
cur.execute('BEGIN')


stats = {}
selects = []
population_total = 0
population_electoral_total = 0
for m in [r1, r2, r3, r4, r5, r6, r7]:
    result = parse_stat(m.NAME, m.DATA)
    for age, (total, *_) in result:
        population_total += total
        if 18 <= age:
            population_electoral_total += total
    stats[m.OSM_ID] = result


info = get_info_normalized(cur)
sum_pop = 0
osm_ids = {}
for i in info:
    k, region, osm_id, town, pop = i
    sum_pop += sum(pop)
    osm_ids[osm_id] = i + [approx_stat0(pop, town, stats[region])]
    print(*i)
assert sum_pop == 9475174
print('passed', sum_pop)


for m in [r1, r2, r3, r4, r5, r6, r7]:
    str_osm_ids = ','.join(str(osm_id) for osm_id in osm_ids)
    selects.append(f"""(
        SELECT p.osm_id, {m.OSM_ID} AS region, p.tags->'name:be' AS name_be, p.tags->'name:ru' AS name_ru, (p.tags->'population')::integer AS population, ST_MakeValid(p.way) AS geom 
        FROM planet_osm_polygon p
        INNER JOIN planet_osm_polygon b ON ST_Contains(b.way, p.way)
        WHERE b.osm_id = {m.OSM_ID} 
        AND (
            p.osm_id IN ({str_osm_ids})
            --p.tags->'admin_level' IN ('6', '9')
            --OR (p.tags->'admin_level' IN ('4', '6') AND p.tags ? 'place')
            --p.tags->'admin_level' IN ('6')
            --OR p.tags->'place' IN ('city', 'town')
            --OR (p.tags->'place' = 'village' AND p.tags->'name:prefix' IN ('городской посёлок', 'рабочий посёлок', 'курортный посёлок'))
        ) 
        ORDER BY (p.tags->'population')::integer DESC
    )""")

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
CITY_REGIONS_R = {
   ('Мінск', 'Минск'): (-59250, -59252, -59257, -59209, -59202, -59199, -59208, -59246, -59249),
   ('Віцебск', 'Витебск'): (-68616, -68617, -68619),
   ('Магілёў', 'Могилёв'): (-82606, -82607),
   ('Бабруйск', 'Бобруйск'): (-3629221, -3629220),
   ('Гомель', 'Гомель'): (-3628812, -3628815, -3628813, -3628814),
   ('Брэст', 'Брест'): (-3626404, -3626405),
   ('Гродна', 'Гродно'): (-2888067, -2888068),
}
CITY_REGIONS = {osm_id: names for names, osm_ids in CITY_REGIONS_R.items() for osm_id in osm_ids}

features = []
for line in out.getvalue().strip().splitlines():
    name_be, name_ru, population_s, area_s, osm_id_s, region_s, town_s, geom_s = line.strip().split('\t')
    if name_be == '\\N':
        continue
    population = int(population_s) if population_s != '\\N' else None
    area = float(area_s)
    region = int(region_s)
    osm_id = int(osm_id_s)
    town = bool(int(town_s))
    geom = json.loads(geom_s)
    if osm_id in CITY_REGIONS:
        city_be, city_ru = CITY_REGIONS[osm_id]
        name_be = f'{city_be}, {name_be}'
        name_ru = f'{city_ru}, {name_ru}'
    k, _, _, town, (lo_m, lo_w, md_m, md_w, hi_m, hi_w), approx_pop = osm_ids[osm_id]

    # print(population, osm_id, name_be, name_ru, area, region, town_s)

    population_electoral = sum(men + woman for age, (men, woman) in approx_pop.items() if 18 <= age)
    population_electoral_m = sum(men for age, (men, woman) in approx_pop.items() if 18 <= age)
    population_18_29_m = sum(men for age, (men, woman) in approx_pop.items() if 18 <= age <= 29)
    population_30_44_m = sum(men for age, (men, woman) in approx_pop.items() if 30 <= age <= 44)
    population_45_59_m = sum(men for age, (men, woman) in approx_pop.items() if 45 <= age <= 59)
    population_60_85_m = sum(men for age, (men, woman) in approx_pop.items() if 60 <= age)
    population_electoral_w = sum(woman for age, (men, woman) in approx_pop.items() if 18 <= age)
    population_18_29_w = sum(woman for age, (men, woman) in approx_pop.items() if 18 <= age <= 29)
    population_30_44_w = sum(woman for age, (men, woman) in approx_pop.items() if 30 <= age <= 44)
    population_45_59_w = sum(woman for age, (men, woman) in approx_pop.items() if 45 <= age <= 59)
    population_60_85_w = sum(woman for age, (men, woman) in approx_pop.items() if 60 <= age)
    population_18_29 = population_18_29_m + population_18_29_w
    population_30_44 = population_30_44_m + population_30_44_w
    population_45_59 = population_45_59_m + population_45_59_w
    population_60_85 = population_60_85_m + population_60_85_w
    population_electoral_p = round(100 * population_electoral / population_electoral, 1)
    population_18_29_p = round(100 * population_18_29 / population_electoral, 1)
    population_30_44_p = round(100 * population_30_44 / population_electoral, 1)
    population_45_59_p = round(100 * population_45_59 / population_electoral, 1)
    population_60_85_p = round(100 * population_60_85 / population_electoral, 1)
    population_electoral_p_m = round(100 * population_electoral_m / population_electoral, 1)
    population_18_29_p_m = round(100 * population_18_29_m / population_electoral, 1)
    population_30_44_p_m = round(100 * population_30_44_m / population_electoral, 1)
    population_45_59_p_m = round(100 * population_45_59_m / population_electoral, 1)
    population_60_85_p_m = round(100 * population_60_85_m / population_electoral, 1)
    population_electoral_p_w = round(100 * population_electoral_w / population_electoral, 1)
    population_18_29_p_w = round(100 * population_18_29_w / population_electoral, 1)
    population_30_44_p_w = round(100 * population_30_44_w / population_electoral, 1)
    population_45_59_p_w = round(100 * population_45_59_w / population_electoral, 1)
    population_60_85_p_w = round(100 * population_60_85_w / population_electoral, 1)
    population_electoral_x = round(100 * population_electoral / population_electoral_total, 2)
    population_18_29_x = round(100 * population_18_29 / population_electoral_total, 2)
    population_30_44_x = round(100 * population_30_44 / population_electoral_total, 2)
    population_45_59_x = round(100 * population_45_59 / population_electoral_total, 2)
    population_60_85_x = round(100 * population_60_85 / population_electoral_total, 2)

    # approx_list, approx_dict = approx_stat(population, town, stats[region])

    # approx_pop_data = {}
    # for age, (pm, pw) in approx_pop.items():
    #     approx_pop_data[f'population_{age}'] = pm + pw
    #     approx_pop_data[f'population_{age}_m'] = pm
    #     approx_pop_data[f'population_{age}_w'] = pw

    population = lo_m + lo_w + md_m + md_w + hi_m + hi_w
    features.append({
        'type': 'Feature',
        'geometry': geom,
        'properties': {
            'name_be': f'{name_be} / {population_electoral_x}%',
            'name_ru': f'{name_ru} / {population_electoral_x}%',

            # 'population_0_15_m': lo_m,
            # 'population_0_15_w': lo_w,
            # 'population_16_60_m': md_m,
            # 'population_16_55_w': md_w,
            # 'population_61_max_m': hi_m,
            # 'population_56_max_w': hi_w,

            'population': population,
            'population_electoral': population_electoral,
            'population_18_29': population_18_29,
            'population_30_44': population_30_44,
            'population_45_59': population_45_59,
            'population_60_85': population_60_85,
            'population_electoral_f': f'{population_electoral} - {population_electoral_p}% (M: {population_electoral_p_m}% W: {population_electoral_p_w}%) / {population_electoral_x}%',
            'population_18_29_f': f'{population_18_29} - {population_18_29_p}% (M: {population_18_29_p_m}% W: {population_18_29_p_w}%) / {population_18_29_x}%',
            'population_30_44_f': f'{population_30_44} - {population_30_44_p}% (M: {population_30_44_p_m}% W: {population_30_44_p_w}%) / {population_30_44_x}%',
            'population_45_59_f': f'{population_45_59} - {population_45_59_p}% (M: {population_45_59_p_m}% W: {population_45_59_p_w}%) / {population_45_59_x}%',
            'population_60_85_f': f'{population_60_85} - {population_60_85_p}% (M: {population_60_85_p_m}% W: {population_60_85_p_w}%) / {population_60_85_x}%',

            'area': round(area, 2),
            'density': round(population / area, 2),
            'density_electoral': round(population_electoral / area, 2),
            'density_18_29': round(population_18_29 / area, 2),
            'density_30_44': round(population_30_44 / area, 2),
            'density_45_59': round(population_45_59 / area, 2),
            'density_60_85': round(population_60_85 / area, 2),

            # **approx_dict,
            # **approx_pop_data,
        }
    })

with open('geo_pop_2019.geojson', 'w') as h:
    json.dump({'type': 'FeatureCollection', 'features': features}, h, indent=2, ensure_ascii=False)
