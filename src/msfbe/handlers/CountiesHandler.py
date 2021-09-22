"""
Copyright (c) 2018 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import json
from msfbe.webmodel import BaseHandler, service_handler
import requests
import psycopg2

class CountiesColumns:
    COUNTY_ID = 0
    NAME = 1
    AREA = 2
    PERIMETER = 3
    CACOA = 4
    CACOA_ID = 5
    DSSLV = 6
    CONUM = 7


class SimpleResult(object):
    def __init__(self, result):
        self.result = result

    def toJson(self):
        return json.dumps(self.result)

@service_handler
class CountiesHandlerImpl(BaseHandler):
    name = "Counties Service"
    path = "/counties"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def __query(self, config, maxLat, maxLon, minLat, minLon):
        sql = """
    select
      county_id,
      name,
      area,
      perimeter,
      cacoa,
      cacoa_id,
      dsslv,
      conum
    from
      counties as c
    where
      ST_Intersects(c.county_shape, ST_MakeEnvelope(%s, %s, %s, %s, 4326));
            """

        conn = psycopg2.connect(dbname=config.get("database", "db.database"),
                                user=config.get("database", "db.username"),
                                password=config.get("database", "db.password"),
                                host=config.get("database", "db.endpoint"),
                                port=config.get("database", "db.port"))
        cur = conn.cursor()

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

    def __format_results(self, rows):
        results = []
        for row in rows:
            results.append({
                "county_id": row[CountiesColumns.COUNTY_ID],
                "name": row[CountiesColumns.NAME],
                "area": row[CountiesColumns.AREA],
                "perimeter": row[CountiesColumns.PERIMETER],
                "cacoa": row[CountiesColumns.CACOA],
                "cacoa_id": row[CountiesColumns.CACOA_ID],
                "dsslv": row[CountiesColumns.DSSLV],
                "conum": row[CountiesColumns.CONUM],
            })
        return results


    def handle(self, computeOptions, **args):
        maxLat = computeOptions.get_decimal_arg("maxLat", 90)
        maxLon = computeOptions.get_decimal_arg("maxLon", 180)
        minLat = computeOptions.get_decimal_arg("minLat", -90)
        minLon = computeOptions.get_decimal_arg("minLon", -180)


        rows = self.__query(args["webconfig"], maxLat, maxLon, minLat, minLon)

        results = self.__format_results(rows)
        return SimpleResult(results)
