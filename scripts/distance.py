import os
import pandas as pd
# import geopandas as gpd
from sqlalchemy import create_engine, text
import pyproj
from pyproj.transformer import TransformerGroup
from pyproj import Transformer, CRS
# import geopy.distance
DB_URI = os.getenv('DB_URI')
db = create_engine(DB_URI)


def check_lib_versions():
    print(f'pyproj -> {pyproj.__version__}')
    print(f'pandas -> {pd.__version__}')


def check_transformations():
    tg = TransformerGroup('epsg:4326', 'epsg:27700')
    if not tg.best_available:
        tg.download_grids(verbose=True)
        return True
    return True


def compare_coordinates(s):
    '''
     1. intersect with TOID layer -> a
     2. within (match_latitude, match_longitude) in a -> b
     if not b
        3. calculate buffer zone

    :param s:
    :return:
    '''

    layer = 'building_toids_test'
    gb_grid_27700 = pyproj.Proj('+init=epsg:27700')
    wgs84 = pyproj.Proj('+init=epsg:4326')

    g_latlng = s['gmap_coord_input']

    try:
        lat, lng = g_latlng.split(',')
        x, y = pyproj.transform(wgs84, gb_grid_27700, lng, lat)
        sql = f'''
        select fid
        from {layer} btt 
        where ST_Intersects(geometry, ST_GeomFromText('SRID=27700;POINT({x} {y})'));
        '''

        # r = db.engine.execute(text(sql))

        with db.connect() as con:
            rs = con.execute(text(sql))
            toids = []
            for row in rs:
                toids.append(row[0])
            t = set(toids)
            r = list(t)
            res = ','.join(r)
        if res:
            # res = r.mappings().all()
            # res_arr = []
            s['toid'] = res
            s['rooftop'] = True
        else:
            sql_buff_250m = f'''
            with pnt as (
                select ST_GeomFromText('SRID=27700;POINT({x} {y})') as pnt
            ),
            toids as (
                select fid, geometry 
                from {layer} btt , pnt as p
                where ST_Intersects(geometry, ST_Buffer(p.pnt, 250, 'quad_segs=8'))
            )
            select 
                t.fid, 
                ST_Distance(t.geometry, p.pnt) As dist
            FROM toids as t, pnt as p
            order by dist asc
            limit 1;
            '''
            with db.connect() as con:
                rs = con.execute(text(sql_buff_250m))
                for row in rs:
                    print(row)
                    res = row
            if res:
                s['toid'] = res[0]
                s['dist'] = res[1]

        # coord2 = eval(g_latlng)
        # gcd = geopy.distance.GreatCircleDistance(coords_1, coords_2).m
        # gd = geopy.distance.GeodesicDistance(coords_1, coords_2).m
        # avg_distance =
        # s['distance_meters'] = (gcd+gd)/2.00
        # s['']

        return s
    except Exception as ex:
        s['error'] = str(ex)
        return s


def distance_to_toid(s):
    layer = 'building_toids_test'
    gb_grid_27700 = pyproj.Proj('+init=epsg:27700')
    wgs84 = pyproj.Proj('+init=epsg:4326')

    try:
        lat = float(s['match_latitude'])
        lng = float(s['match_longitude'])
        toid = str(s['toid'])

        x, y = pyproj.transform(wgs84, gb_grid_27700, lng, lat)
        sql_dist = f'''
        with pnt as (
            select ST_GeomFromText('SRID=27700;POINT({x} {y})') as pnt
        ),
        toids as (
            select fid, geometry 
            from {layer} btt
            where fid = '{toid}'
        )
        select 
            ST_Distance(t.geometry, p.pnt) As dist
        FROM toids as t, pnt as p
        order by dist asc
        limit 1;
        '''
        print(sql_dist)
        with db.connect() as con:
            rs = con.execute(text(sql_dist))
            for row in rs:
                print(row)
                res = row
        if res:
            s['distance_to_toid'] = res[0]

        return s
    except Exception as ex:
        s['error_dist'] = str(ex)
        return s


def dist_to_cols(s):
    # within 0m, 10m, 50m, 150 m
    try:
        dist = float(s['distance_to_toid'])
        if dist == 0:
            s['dist_0m'] = True

        if dist > 0 and dist < 10:
            s['dist_0_10m'] = True

        if dist > 10 and dist < 50:
            s['dist_10_50m'] = True

        if dist > 50 and dist < 100:
            s['dist_50_100m'] = True

        if dist > 100 and dist < 150:
            s['dist_100_150m'] = True

        if dist > 150 and dist < 250:
            s['dist_150_250m'] = True

        if dist > 250:
            s['dist_more_than_250m'] = True
        return s
    except Exception as ex:
        s['dist_to_cols_error'] = str(ex)


if __name__ == '__main__':
    check_lib_versions()
    check_transformations()
    df = pd.read_csv('/home/cytoragis/gis-preps/scripts/UK_labeled_uk_address_labelling_marsh_all_lat_long_with_toid_distance_v2.csv')
    #df = df.apply(compare_coordinates, axis=1)
    #df.to_excel('uk_address_labelling_marsh_all_lat_long_with_toid_distance_v2__.xlsx')
    #df.to_csv('uk_address_labelling_marsh_all_lat_long_with_toid_distance_v2__.csv')
    #df = df.apply(distance_to_toid, axis=1)
    #df.to_excel('uk_address_labelling_marsh_all_lat_long_with_toid_distance_v2.xlsx')
    #df.to_csv('uk_address_labelling_marsh_all_lat_long_with_toid_distance_v2.csv')
    df = df.apply(dist_to_cols, axis=1)
    df.to_excel('rel.xlsx')


    print(df.info)
