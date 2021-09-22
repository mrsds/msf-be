"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import json
from msfbe.webmodel import BaseHandler, service_handler
import psycopg2
from osgeo import gdal,ogr,osr

PLUME_ID = 0
JSON_URL = 1
PNG_URL = 2
PLUME_URL = 3
RGBQLCTR_URL = 4
PNG_URL_THUMB = 5
PLUME_URL_THUMB = 6
RGBQLCTR_URL_THUMB = 7
PLUME_TIFF_URL = 8
RGB_TIFF_URL = 9
DATA_DATE = 10
MERGEDIST = 11
SOURCE_ID = 12
IME_5 = 13
IME_10 = 14
IME_20 = 15
CANDIDATE_ID = 16
PLUME_LONGITUDE = 17
PLUME_LATITUDE = 18
AVIRIS_PLUME_ID = 19
DETID5 = 20
DETID10 = 21
DETID20 = 22
FETCH5 = 23
FETCH10 = 24
FETCH20 = 25
FLUX = 26
FLUX_UNCERTAINTY = 27
PLUME_SHAPE_WKT = 28


FLIGHTLINE_ID = 0
FLIGHT_TIMESTAMP = 1
FLIGHT_NAME = 2
FLIGHT_IMAGE_URL = 3
FLIGHT_SHAPE_WKT = 4

class SimpleJsonResult(object):
    def __init__(self, result):
        self.result = result

    def toJson(self):
        return json.dumps(self.result)


def replace_s3_url(url, s3url):
    if url is not None:
        url = url.replace("{s3}/", "{s3}")
        return url.replace("{s3}", s3url)
    else:
        return url

@service_handler
class AvirisPlumeHandlerImpl(BaseHandler):
    name = "AVIRIS Plume Service"
    path = "/aviris/plumes"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)


    def __coord_array_to_polygon(self, coords):
        item = {
            "type": "Feature",
            "properties": {

            }
        }
        item.update({
            "geometry": {
                "type": "Polygon",
                "coordinates": coords
            }
        })

        return item


    def __query(self, config, maxLat, maxLon, minLat, minLon, maxObjects, source_id, plume_id, candidate_id):


        conn = psycopg2.connect(dbname=config.get("database", "db.database"),
                                user=config.get("database", "db.username"),
                                password=config.get("database", "db.password"),
                                host=config.get("database", "db.endpoint"),
                                port=config.get("database", "db.port"))
        cur = conn.cursor()

        sql = """
select
  ap.plume_id,
  ap.json_url,
  ap.png_url,
  ap.plume_url,
  ap.rgbqlctr_url,
  ap.png_url_thumb,
  ap.plume_url_thumb,
  ap.rgbqlctr_url_thumb,
  ap.plume_tiff_url,
  ap.rgb_tiff_url,
  to_char(ap.data_date, 'yyyy-mm-dd HH24:MI:SS'),
  ap.mergedist,
  p.source_id,
  ap.ime_5,
  ap.ime_10,
  ap.ime_20,
  ap.candidate_id,
  p.plume_longitude_deg,
  p.plume_latitude_deg,
  ap.aviris_plume_id,
  ap.detid5,
  ap.detid10,
  ap.detid20,
  ap.fetch5,
  ap.fetch10,
  ap.fetch20,
  p.flux,
  p.flux_uncertainty,
  ST_asText(ap.plume_shape) as plume_shape_wkt
from
  aviris_plumes as ap,
  plumes as p
where
  ST_Intersects(ap.plume_shape, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
  and ap.source_id is not null
  and ap.candidate_id is not null
  and p.plume_id = ap.candidate_id
  {sourceidsql}
  {plumeidsql}
  {candidateidsql}
        """

        if source_id is not None:
            source_id = "'%s'" % "','".join(source_id)
            sourceidsql = "and ap.source_id in (%s)"%source_id
        else:
            sourceidsql = ""

        if plume_id is not None:
            plumeidsql = "and ap.plume_id=%s" % plume_id
        else:
            plumeidsql = ""

        if candidate_id is not None:
            candidateidsql = "and ap.candidate_id=\'%s\'" % candidate_id
        else:
            candidateidsql = ""



        sql = sql.format(sourceidsql=sourceidsql, plumeidsql=plumeidsql, candidateidsql=candidateidsql)

        # Query
        cur.execute(sql,
                    (
                        minLon,
                        minLat,
                        maxLon,
                        maxLat
                    )
                    )

        results = cur.fetchall()

        cur.close()
        conn.close()

        return results


    def __format_plume(self, row, s3url):
        plume = {
            "id": row[PLUME_ID],
            "json_url": replace_s3_url(row[JSON_URL], s3url) if row[JSON_URL] is not None else "n/a",
            "png_url": replace_s3_url(row[PNG_URL], s3url),
            "plume_url": replace_s3_url(row[PLUME_URL], s3url),
            "rgbqlctr_url": replace_s3_url(row[RGBQLCTR_URL], s3url),
            "png_url_thumb": replace_s3_url(row[PNG_URL_THUMB], s3url),
            "plume_url_thumb": replace_s3_url(row[PLUME_URL_THUMB], s3url),
            "rgbqlctr_url_thumb": replace_s3_url(row[RGBQLCTR_URL_THUMB], s3url),
            "plume_tiff_url": replace_s3_url(row[PLUME_TIFF_URL], s3url),
            "rgb_tiff_url": replace_s3_url(row[RGB_TIFF_URL], s3url),
            "data_date_dt": row[DATA_DATE],
            "mergedist": row[MERGEDIST],
            "source_id": row[SOURCE_ID] if row[SOURCE_ID] is not None else "n/a",
            "ime_5": row[IME_5],
            "ime_10": row[IME_10],
            "ime_20": row[IME_20],
            "candidate_id": row[CANDIDATE_ID] if row[CANDIDATE_ID] is not None else "n/a",
            "flight_campaign": row[CANDIDATE_ID][:row[CANDIDATE_ID].index("-")] if row[CANDIDATE_ID] is not None else "n/a",
            "plume_id": row[AVIRIS_PLUME_ID],
            "detid5": row[DETID5],
            "detid10": row[DETID10],
            "detid20": row[DETID20],
            "fetch5": row[FETCH5],
            "fetch10": row[FETCH10],
            "fetch20": row[FETCH20],
            "flux": row[FLUX],
            "flux_uncertainty": row[FLUX_UNCERTAINTY],
            "shape": [],
            "location": [
                row[PLUME_LATITUDE], row[PLUME_LONGITUDE]
            ]
        }

        shape_wkt = row[PLUME_SHAPE_WKT]
        shape_geom = ogr.CreateGeometryFromWkt(shape_wkt)
        shape_geom_poly = shape_geom.GetGeometryRef(0)
        for i in range(0, shape_geom_poly.GetPointCount()):
            # GetPoint returns a tuple not a Geometry
            pt = shape_geom_poly.GetPoint(i)
            plume["shape"].append(pt[:2])



        return plume


    def handle(self, computeOptions, **args):
        maxLat = computeOptions.get_decimal_arg("maxLat", 90)
        maxLon = computeOptions.get_decimal_arg("maxLon", 180)
        minLat = computeOptions.get_decimal_arg("minLat", -90)
        minLon = computeOptions.get_decimal_arg("minLon", -180)

        count_only = computeOptions.get_boolean_arg("countonly", False)
        maxObjects = computeOptions.get_argument("maxObjects", 10)

        plume_id = computeOptions.get_argument("id", None)
        source_id = computeOptions.get_argument("source", None)
        candidate_id = computeOptions.get_argument("cid", None)
        if type(source_id) == str or type(source_id) == unicode:
            source_id = source_id.split(",")

        rows = self.__query(args["webconfig"], maxLat, maxLon, minLat, minLon, maxObjects, source_id, plume_id, candidate_id)

        s3url = args["webconfig"].get("s3", "s3.proxyurl")
        results = [self.__format_plume(row, s3url) for row in rows]

        if count_only is True:
            return SimpleJsonResult({
                "count": len(results)
            })
        else:
            return SimpleJsonResult(results)



@service_handler
class AvirisFlightHandlerImpl(BaseHandler):
    name = "AVIRIS Flightline Service"
    path = "/aviris/flights"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)


    def __query(self, config, maxLat, maxLon, minLat, minLon, maxObjects=1000):


        conn = psycopg2.connect(dbname=config.get("database", "db.database"),
                                user=config.get("database", "db.username"),
                                password=config.get("database", "db.password"),
                                host=config.get("database", "db.endpoint"),
                                port=config.get("database", "db.port"))
        cur = conn.cursor()

        sql = """
select
  f.flightline_id,
  to_char(f.flight_timestamp, 'yyyy-mm-dd HH24:MI:SS') as flight_timestamp,
  f.flight_name,
  f.image_url as flight_image_url,
  ST_asText(f.flightline_shape) as flight_shape_wkt
from
  flightlines as f
where
  ST_Intersects(f.flightline_shape, ST_MakeEnvelope(%s, %s, %s, %s, 4326))       
limit %s;
        """


        cur.execute(sql,
                    (
                        minLon,
                        minLat,
                        maxLon,
                        maxLat,
                        maxObjects
                    )
                    )

        results = cur.fetchall()

        cur.close()
        conn.close()

        return results




    def __format_flight_basic(self, row, s3url):
        flight = {
            "name": replace_s3_url(row[FLIGHT_NAME], s3url),
            "shape": [],
            "png_url" : row[FLIGHT_IMAGE_URL],
            "data_date_dt": row[FLIGHT_TIMESTAMP],
            "id": row[FLIGHTLINE_ID]
        }

        shape_wkt = row[FLIGHT_SHAPE_WKT]
        shape_geom = ogr.CreateGeometryFromWkt(shape_wkt)
        shape_geom_poly = shape_geom.GetGeometryRef(0)
        for i in range(0, shape_geom_poly.GetPointCount()):
            # GetPoint returns a tuple not a Geometry
            pt = shape_geom_poly.GetPoint(i)
            flight["shape"].append(pt[:2])


        return flight


    def __wkt_to_geojson(self, wkt):
        shape_geom = ogr.CreateGeometryFromWkt(wkt)
        return json.loads(shape_geom.ExportToJson())

    def __format_flight_geojson(self, row, s3url):
        geojson = self.__wkt_to_geojson(row[FLIGHT_SHAPE_WKT])
        geojson["properties"] = {
            "name": row[FLIGHT_NAME],
            "png_url": replace_s3_url(row[FLIGHT_IMAGE_URL], s3url),
            "data_date_dt": row[FLIGHT_TIMESTAMP],
            "id": row[FLIGHTLINE_ID]
        }
        return geojson

    def __format_rows_basic(self, rows, s3url):
        results = []
        for row in rows:
            results.append(self.__format_flight_basic(row, s3url))
        return results

    def __format_rows_geojson(self, rows, s3url):

        features = []
        for row in rows:
            features.append(self.__format_flight_geojson(row, s3url))

        geojson = {
            'type': 'FeatureCollection',
            'crs': {
                'type': 'name',
                'properties': {
                    'name': "EPSG:4326"
                }
            },
            "features": features
        }

        return geojson

    def __format_rows(self, rows, s3url, as_geojson=True):
        if as_geojson:
            return self.__format_rows_geojson(rows, s3url)
        else:
            return self.__format_rows_basic(rows, s3url)


    def handle(self, computeOptions, **args):
        maxLat = computeOptions.get_decimal_arg("maxLat", 90)
        maxLon = computeOptions.get_decimal_arg("maxLon", 180)
        minLat = computeOptions.get_decimal_arg("minLat", -90)
        minLon = computeOptions.get_decimal_arg("minLon", -180)
        count_only = computeOptions.get_boolean_arg("countonly", False)
        as_geojson = computeOptions.get_boolean_arg("asgeojson", True)

        maxObjects = computeOptions.get_argument("maxObjects", 1000)

        rows = self.__query(args["webconfig"], maxLat, maxLon, minLat, minLon, maxObjects)

        s3url = args["webconfig"].get("s3", "s3.proxyurl")
        results = self.__format_rows(rows, s3url, as_geojson)

        if count_only is True:
            return SimpleJsonResult({
                "count": len(results)
            })
        else:
            return SimpleJsonResult(results)