"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import json
from msfbe.webmodel import BaseHandler, service_handler
import psycopg2
from osgeo import gdal,ogr,osr


VISTA_ID = 0
VISTA_NAME = 1
SITE_NAME = 2
SHAPE_TYPE = 3
LATITUDE = 4
LONGITUDE = 5
CATEGORY = 6
CATEGORY_ID = 7
OPERATOR = 8
ADDRESS = 9
STATE = 10
SECTOR = 11
CITY = 12
SOURCE_ID = 13
FACILITY_POLY_CONTAINS_SOURCE = 14
DISTANCE = 15
AREA_NAME = 16
SOURCE_TYPE = 17
SOURCE_LATITUDE = 18
SOURCE_LONGITUDE = 19
SECTOR_LEVEL_1 = 20
SECTOR_LEVEL_2 = 21
SECTOR_LEVEL_3 = 22
NEAREST_FACILITY = 23
FLYOVER_COUNT = 24
PLUME_COUNT = 25
PLUME_SHAPE_WKT = 26
GEOJSON = 27
INTERNAL_ID = 28
PROPERTY_NAME = 28
PROPERTY_VALUE = 29



class FieldBoundaryColumns:
    ID = 0
    NAME = 1
    AREA_SQ_MI = 2
    AREA_ACRE = 3
    PERIMETER = 4
    DISTRICT = 5
    FIELD_ENVELOPE = 6
    FIELD_SHAPE = 7


class VistaMetadataColumns:
    ID = 0
    PROPERTY_NAME = 1
    PROPERTY_VALUE = 2


@service_handler
class VistaHandlerImpl(BaseHandler):
    name = "Vista Service"
    path = "/vista"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    @staticmethod
    def __open_db_connection(config):
        conn = psycopg2.connect(dbname=config.get("database", "db.database"),
                                user=config.get("database", "db.username"),
                                password=config.get("database", "db.password"),
                                host=config.get("database", "db.endpoint"),
                                port=config.get("database", "db.port"))
        cur = conn.cursor()

        return conn, cur

    @staticmethod
    def __close_db_connection(conn, cur):
        cur.close()
        conn.close()

    @staticmethod
    def __parse_field_query_results(cur):
        results = []

        while True:
            row = cur.fetchone()
            if row is None:
                break

            shape_wkt = row[FieldBoundaryColumns.FIELD_SHAPE]
            shape_geom = ogr.CreateGeometryFromWkt(shape_wkt)
            geojson = json.loads(shape_geom.ExportToJson())

            item = {
                "type": "Feature",
                "properties": {
                    "name": row[FieldBoundaryColumns.NAME],
                    "area_sq_mi": row[FieldBoundaryColumns.AREA_SQ_MI],
                    "area_acre": row[FieldBoundaryColumns.AREA_ACRE],
                    "perimeter": row[FieldBoundaryColumns.PERIMETER],
                    "distict": row[FieldBoundaryColumns.DISTRICT],
                    "category": "Field_Boundaries",
                    "category_id": 1000
                },
                "geometry" : geojson,
                "id": row[FieldBoundaryColumns.ID]
            }

            results.append(item)

        return results

    @staticmethod
    def __parse_vista_query_results(cur, maxObjects, includeProperties=False):
        results = []

        curr_vista_id = None

        while True:
            row = cur.fetchone()
            if row is None:
                break
            if row[VISTA_ID] != curr_vista_id:
                if len(results) >= maxObjects:
                    break
                item = json.loads(row[GEOJSON])
                item["properties"] = {
                    "name": row[VISTA_NAME],
                    "id": row[VISTA_ID],
                    "internal_id": row[INTERNAL_ID],
                    "category_id": row[CATEGORY_ID],
                    "category": row[CATEGORY],
                    "num_flights_matching": row[FLYOVER_COUNT],
                    "num_plumes_matching": row[PLUME_COUNT],
                    "description": None,
                    "metadata": {
                        "LLat": str(round(row[LATITUDE] * 100000) / 100000),
                        "LLong": str(round(row[LONGITUDE] * 100000) / 100000)
                    },
                    "sources": {}
                }
                results.append(item)
                curr_vista_id = row[VISTA_ID]


            if includeProperties:
                item["properties"]["metadata"][row[PROPERTY_NAME]] = row[PROPERTY_VALUE]


            if row[SOURCE_ID] is not None:
                item["properties"]["sources"][row[SOURCE_ID]] = {
                    "id": row[SOURCE_ID],
                    "lat": row[SOURCE_LATITUDE],
                    "lon": row[SOURCE_LONGITUDE],
                    "area": row[AREA_NAME],
                    "type": row[SOURCE_TYPE],
                    "est_dist_from_facility": row[DISTANCE],
                    "sector_level_1": row[SECTOR_LEVEL_1],
                    "sector_level_2": row[SECTOR_LEVEL_2],
                    "sector_level_3": row[SECTOR_LEVEL_3],
                    "internal_id": row[SOURCE_ID]
                }

        return results

    @staticmethod
    def __parse_vista_metadata_query_results(cur, results={}):
        while True:
            row = cur.fetchone()
            if row is None:
                break
            results[row[VistaMetadataColumns.PROPERTY_NAME]] = row[VistaMetadataColumns.PROPERTY_VALUE]
        return results

    @staticmethod
    def __query_vista_metadata(cur, vista_id, results={}):
        sql = """
        select vista_id, property_name, property_value from vista_metadata where vista_id = %s;
        """
        cur.execute(sql, (vista_id,))
        results = VistaHandlerImpl.__parse_vista_metadata_query_results(cur, results)

        return results

    @staticmethod
    def __query_single_object(config, vista_id):
        conn, cur = VistaHandlerImpl.__open_db_connection(config)

        sql = """
        select
          v.vista_id,
          v.name,
          v.site_name,
          v.shape_type,
          v.latitude,
          v.longitude,
          v.category,
          v.category_id,
          v.operator,
          v.address,
          v.state,
          v.sector,
          v.city,
          vs.source_id,
          vs.facility_poly_contains_source,
          vs.distance,
          s.area_name,
          s.source_type,
          s.source_latitude_deg,
          s.source_longitude_deg,
          s.sector_level_1,
          s.sector_level_2,
          s.sector_level_3,
          s.nearest_facility,
          vf.flyover_count,
          vap.plume_count,
          ST_asText(v.facility_envelope) as plume_shape_wkt,
          v.geojson,
          v.id
        from
          vista as v
          left join vista_sources as vs
              on vs.vista_id = v.id
          left join sources as s
              on s.source_id = vs.source_id
          left join (select distinct vista_id, count(1) as flyover_count from vista_flightlines group by vista_id) as vf
              on vf.vista_id = v.id
          left join (select distinct vista_id, count(1) as plume_count from vista_aviris_plumes group by vista_id) as vap
              on vap.vista_id = v.id
        where
          v.vista_id = %s;
                """

        cur.execute(sql, (vista_id,))

        results = VistaHandlerImpl.__parse_vista_query_results(cur, 1, includeProperties=False)

        if len(results) == 1:
            internal_id = results[0]["properties"]["internal_id"]
            results[0]["properties"]["metadata"] = VistaHandlerImpl.__query_vista_metadata(cur, internal_id, {"LLat":results[0]["properties"]["metadata"]["LLat"], "LLong":results[0]["properties"]["metadata"]["LLong"]})

        VistaHandlerImpl.__close_db_connection(conn, cur)

        return results

    @staticmethod
    def __query_fields(config, maxLat, maxLon, minLat, minLon):
        conn, cur = VistaHandlerImpl.__open_db_connection(config)

        sql = """
        select
  fb.id,
  fb.feature_name,
  fb.area_sq_mi,
  fb.area_acre,
  fb.perimeter,
  fb.district,
  ST_asText(fb.field_envelope) as field_envelope_wkt,
  ST_asText(fb.field_shape) as field_shape_wkt
from
  field_boundaries as fb
where
  ST_Intersects(fb.field_envelope , ST_MakeEnvelope(%s, %s, %s, %s, 4326));
        """

        cur.execute(sql,
                    (
                        minLon,
                        minLat,
                        maxLon,
                        maxLat
                    )
                    )

        results = VistaHandlerImpl.__parse_field_query_results(cur)
        VistaHandlerImpl.__close_db_connection(conn, cur)

        return results

    @staticmethod
    def __query(config, maxLat, maxLon, minLat, minLon, category, maxObjects, source_id):
        conn, cur = VistaHandlerImpl.__open_db_connection(config)

        sql = """
select
  v.vista_id,
  v.name,
  v.site_name,
  v.shape_type,
  v.latitude,
  v.longitude,
  v.category,
  v.category_id,
  v.operator,
  v.address,
  v.state,
  v.sector,
  v.city,
  vs.source_id,
  vs.facility_poly_contains_source,
  vs.distance,
  s.area_name,
  s.source_type,
  s.source_latitude_deg,
  s.source_longitude_deg,
  s.sector_level_1,
  s.sector_level_2,
  s.sector_level_3,
  s.nearest_facility,
  vf.flyover_count,
  vap.plume_count,
  ST_asText(v.facility_envelope) as plume_shape_wkt,
  v.geojson,
  v.id
from
  vista as v
  left join vista_sources as vs
      on vs.vista_id = v.id
  left join sources as s
      on s.source_id = vs.source_id
  left join (select distinct vista_id, count(1) as flyover_count from vista_flightlines group by vista_id) as vf
      on vf.vista_id = v.id
  left join (select distinct vista_id, count(1) as plume_count from vista_aviris_plumes group by vista_id) as vap
      on vap.vista_id = v.id
where
  ST_Intersects(v.facility_envelope, ST_MakeEnvelope(%s, %s, %s, %s, 4326))        
        {sourceidsql}
        {categoryidsql};
        """

        if source_id is not None:
            source_id = "'%s'" % "','".join(source_id)
            sourceidsql = " and vs.source_id in (%s) " % source_id
        else:
            sourceidsql = ""

        if category is not None:
            categoryidsql = " and v.category_id in (%s) " % ", ".join(map(str, category))
        else:
            categoryidsql = ""

        sql = sql.format(sourceidsql=sourceidsql, categoryidsql=categoryidsql)


        cur.execute(sql,
                    (
                        minLon,
                        minLat,
                        maxLon,
                        maxLat
                    )
                    )

        results = VistaHandlerImpl.__parse_vista_query_results(cur, maxObjects, includeProperties=False)

        VistaHandlerImpl.__close_db_connection(conn, cur)

        return results

    def handle(self, computeOptions, **args):

        maxLat = computeOptions.get_decimal_arg("maxLat", 90)
        maxLon = computeOptions.get_decimal_arg("maxLon", 180)
        minLat = computeOptions.get_decimal_arg("minLat", -90)
        minLon = computeOptions.get_decimal_arg("minLon", -180)
        category = computeOptions.get_argument("category", (range(0, 14) + [1000,]))
        count_only = computeOptions.get_boolean_arg("countonly", False)
        source_id = computeOptions.get_argument("source", None)
        vista_id = computeOptions.get_argument("vistaId", None)
        if type(source_id) == str or type(source_id) == unicode:
            source_id = source_id.split(",")

        if type(category) == str or type(category) == unicode:
            category = map(int, category.split(","))

        maxObjects = computeOptions.get_int_arg("maxObjects", 1000)

        if vista_id is None:
            results = self.__query(args["webconfig"], maxLat, maxLon, minLat, minLon, category, maxObjects, source_id)
            #if 1000 in category:
            #    results_fields = self.__query_fields(args["webconfig"], maxLat, maxLon, minLat, minLon)
            #else:
            #    results_fields = []
            #results = results_vista + results_fields
        else:
            results = self.__query_single_object(args["webconfig"], vista_id)

        class SimpleResult(object):
            def __init__(self, result):
                self.result = result

            def toJson(self):
                return json.dumps(self.result)

        geojson = results#self.__response_to_geojson(response)

        if count_only is True:
            return SimpleResult({
                "count": len(geojson)
            })
        else:
            geojson = {
                'type': 'FeatureCollection',
                'crs': {
                    'type':'name',
                    'properties': {
                        'name': "EPSG:4326"
                    }
                },
                "features": geojson
            }

        return SimpleResult(geojson)