"""
Copyright (c) 2018 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import json
from msfbe.webmodel import BaseHandler, service_handler, SimpleResults
import psycopg2
from osgeo import gdal,ogr,osr


class SourceListColumns:
    SOURCE_ID = 0
    SOURCE_LATITUDE_DEG = 1
    SOURCE_LONGITUDE_DEG = 2
    AREA_NAME = 3
    SOURCE_TYPE = 4
    NEAREST_FACILITY = 5
    SELECTION_CRITERIA = 6
    SECTOR_LEVEL_1 = 7
    SECTOR_LEVEL_2 = 8
    SECTOR_LEVEL_3 = 9
    VISTA_ID = 10
    DISTANCE = 11
    VISTA_NAME = 12
    VISTA_LATITUDE = 13
    VISTA_LONGITUDE = 14
    CATEGORY = 15
    CATEGORY_ID = 16
    CONFIDENCE_IN_PERSISTENCE = 17


@service_handler
class SourcesHandlerImpl(BaseHandler):
    name = "Sources Service"
    path = "/sources"
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
  s.source_id,
  s.source_latitude_deg,
  s.source_longitude_deg,
  s.area_name,
  s.source_type,
  s.nearest_facility,
  s.selection_criteria,
  s.sector_level_1,
  s.sector_level_2,
  s.sector_level_3,
  vs.vista_id,
  vs.distance,
  v.name,
  v.latitude,
  v.longitude,
  v.category,
  v.category_id,
  s.confidence_in_persistence
from
     sources as s
     left join (
         select distinct
          vs.source_id,
          min(vs.distance) as min_dist
        from
          vista_sources as vs
        group by
          vs.source_id
      ) as vsm
      on vsm.source_id = s.source_id
  left join vista_sources as vs
      on vs.source_id = vsm.source_id
        and vs.distance = vsm.min_dist
  left join vista as v
      on v.id = vs.vista_id
where
  ST_Intersects(s.source_location, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
limit
  %s
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

    def __format_source(self, row):
        source = {
            "source_id": row[SourceListColumns.SOURCE_ID],
            "source_latitude": row[SourceListColumns.SOURCE_LATITUDE_DEG],
            "source_longitude": row[SourceListColumns.SOURCE_LONGITUDE_DEG],
            "area_name": row[SourceListColumns.AREA_NAME],
            "source_type": row[SourceListColumns.SOURCE_TYPE],
            "nearest_facility": row[SourceListColumns.NEAREST_FACILITY],
            "selection_criteria": row[SourceListColumns.SELECTION_CRITERIA],
            "sector_level_1": row[SourceListColumns.SECTOR_LEVEL_1],
            "sector_level_2": row[SourceListColumns.SECTOR_LEVEL_2],
            "sector_level_3": row[SourceListColumns.SECTOR_LEVEL_3],
            "nearest_vista_id": row[SourceListColumns.VISTA_ID],
            "nearest_vista_distance": row[SourceListColumns.DISTANCE],
            "nearest_vista_name": row[SourceListColumns.VISTA_NAME],
            "nearest_vista_latitude": row[SourceListColumns.VISTA_LATITUDE],
            "nearest_vista_longitude": row[SourceListColumns.VISTA_LONGITUDE],
            "vista_category": row[SourceListColumns.CATEGORY],
            "vista_category_id": row[SourceListColumns.CATEGORY_ID],
            "confidence_in_persistence": row[SourceListColumns.CONFIDENCE_IN_PERSISTENCE]
        }
        return source


    def handle(self, computeOptions, **args):
        maxLat = computeOptions.get_decimal_arg("maxLat", 90)
        maxLon = computeOptions.get_decimal_arg("maxLon", 180)
        minLat = computeOptions.get_decimal_arg("minLat", -90)
        minLon = computeOptions.get_decimal_arg("minLon", -180)

        maxObjects = computeOptions.get_argument("maxObjects", 1000)


        rows = self.__query(args["webconfig"], maxLat, maxLon, minLat, minLon, maxObjects)

        results = [self.__format_source(row) for row in rows]


        return SimpleResults(results)