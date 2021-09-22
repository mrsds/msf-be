"""
Copyright (c) 2018 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import json
from msfbe.webmodel import BaseHandler, service_handler
from datetime import datetime
import psycopg2
from msfbe.queryhandlers import *



class DetectionRatesBySectorColumns:
    SECTOR_LEVEL_1 = 0
    SECTOR_LEVEL_2 = 1
    FACILITIES = 2
    FACILITY_FLYOVERS = 3
    UNIQUE_FACILITIES_FLOWN_OVER = 4
    UNIQUE_FACILITIES_WITH_PLUME_DETECTIONS = 5

class SimpleResult(object):
    def __init__(self, result):
        self.result = result

    def toJson(self):
        return json.dumps(self.result)

@service_handler
class StatsHandlerImpl(BaseHandler):
    name = "Detection Rates by Sector"
    path = "/detectionBySector"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)


    def __format_dt(self, dt):
        return dt.strftime("%Y-%m-%d")

    def __query(self, config, county=None, sector=None, subsector=None, from_date=None, to_date=None):
        conn = psycopg2.connect(dbname=config.get("database", "db.database"),
                                user=config.get("database", "db.username"),
                                password=config.get("database", "db.password"),
                                host=config.get("database", "db.endpoint"),
                                port=config.get("database", "db.port"))
        cur = conn.cursor()

        county = "" if county is None else county
        sector = "" if sector is None else sector
        subsector = "" if subsector is None else subsector

        to_date = "now()" if to_date is None else self.__format_dt(to_date)
        from_date = "1970-01-01" if from_date is None else self.__format_dt(from_date)
        sql = """
select distinct
  v.sector_level_1,
  v.sector_level_2,
  count(distinct v.vista_id) as facilities,
  count(distinct vf.flightline_id) as facility_flyovers,
  count(distinct vf.vista_id) as unique_facilities_flown_over,
  count(distinct p.vista_id) as unique_facilities_with_plume_detections
from
  counties as c,
  county_vista as cv,
  vista as v
  left join (select vf.vista_id, vf.flightline_id from vista_flightlines as vf, flightlines as f where vf.flightline_id = f.flightline_id and f.flight_timestamp between %s and %s) vf
      on v.id = vf.vista_id
  left join (select * from plumes as p where p.detection_timestamp between %s and %s) as p
      on p.vista_id = v.vista_id
where
  c.name like %s
  and cv.county_id = c.county_id
  and v.id = cv.vista_id
  and v.sector_level_1 like %s
  and v.sector_level_2 like %s
group by
  v.sector_level_1,
  v.sector_level_2;
        """

        cur.execute(sql,
                    (
                        from_date,
                        to_date,
                        from_date,
                        to_date,
                        "%{county}%".format(county=county),
                        "%{sector}%".format(sector=sector),
                        "%{subsector}%".format(subsector=subsector)
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
                "sector_level_1": row[DetectionRatesBySectorColumns.SECTOR_LEVEL_1],
                "sector_level_2": row[DetectionRatesBySectorColumns.SECTOR_LEVEL_2],
                "facilities": row[DetectionRatesBySectorColumns.FACILITIES],
                "facility_flyovers": row[DetectionRatesBySectorColumns.FACILITY_FLYOVERS],
                "unique_facilities_flown_over": row[DetectionRatesBySectorColumns.UNIQUE_FACILITIES_FLOWN_OVER],
                "unique_facilities_with_plume_detections": row[DetectionRatesBySectorColumns.UNIQUE_FACILITIES_WITH_PLUME_DETECTIONS]
            })

        return results


    def handle(self, computeOptions, **args):

        county = computeOptions.get_argument("county", None)
        sector = computeOptions.get_argument("sector", None)
        subsector = computeOptions.get_argument("subsector", None)
        from_date = computeOptions.get_datetime_arg("from_date", None)
        to_date = computeOptions.get_datetime_arg("to_date", None)
        rows = self.__query(args["webconfig"], county, sector, subsector, from_date, to_date)
        results = self.__format_results(rows)


        return SimpleResult(results)





"""
s.source_id,
s.total_overflights,
f.flight_name,
to_char(f.flight_timestamp, 'yyyy-mm-dd HH24:MI:SS') as flight_timestamp,
p.plume_id is not null as plume_detected,
p.total_overflights,
p.q_source_final,
p.plume_id,
p.candidate_id,
to_char(p.detection_timestamp, 'yyyy-mm-dd HH24:MI:SS') as detection_timestamp,
p.flux,
p.flux_uncertainty,
p.vista_id,
p.vista_name,
p.plume_longitude_deg,
p.plume_latitude_deg
"""

class FlyoverOfSourceColumns:
    SOURCE_ID = 0
    TOTAL_OVERFLIGHTS = 1
    FLIGHT_NAME = 2
    FLIGHTLINE_TIMESTAMP = 3
    PLUME_DETECTED = 4
    Q_SOURCE_FINAL = 5
    PLUME_ID = 6
    CANDIDATE_ID = 7
    PLUME_DATE = 8
    FLUX = 9
    FLUX_UNCERTAINTY = 10
    VISTA_ID = 11
    VISTA_NAME = 12
    PLUME_LONGITUDE = 13
    PLUME_LATITUDE = 14
    FLIGHTLINE_ID = 15
    PNG_URL = 16
    PLUME_URL = 17
    RGBQLCTR_URL = 18
    PNG_URL_THUMB = 19
    PLUME_URL_THUMB = 20
    RGBQLCTR_URL_THUMB = 21




@service_handler
class FlyoverOfSourceHandlerImpl(BaseHandler):
    name = "Flyovers of Connected Plume Source"
    path = "/flyoversOfPlumeSource"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def __query(self, config, source_id):
        conn = psycopg2.connect(dbname=config.get("database", "db.database"),
                                user=config.get("database", "db.username"),
                                password=config.get("database", "db.password"),
                                host=config.get("database", "db.endpoint"),
                                port=config.get("database", "db.port"))
        cur = conn.cursor()
        sql = """
select
  s.source_id,
  s.total_overflights,
  f.flight_name,
  to_char(f.flight_timestamp, 'yyyy-mm-dd HH24:MI:SS') as flight_timestamp,
  p.plume_id is not null as plume_detected,
  p.q_source_final,
  p.plume_id,
  p.candidate_id,
  to_char(p.detection_timestamp, 'yyyy-mm-dd HH24:MI:SS') as detection_timestamp,
  p.flux,
  p.flux_uncertainty,
  p.vista_id,
  '' as vista_name,
  p.plume_longitude_deg,
  p.plume_latitude_deg,
  f.flightline_id,
  ap.png_url,
  ap.plume_url,
  ap.rgbqlctr_url,
  ap.png_url_thumb,
  ap.plume_url_thumb,
  ap.rgbqlctr_url_thumb
from
  sources as s
  left join sources_flightlines as sf on s.source_id = sf.source_id
  left join flightlines f on sf.flightline_id = f.flightline_id
  left join (
      select
        s.source_id,
        s.total_overflights,
        s.q_source_final,
        p.plume_id,
        p.candidate_id,
        p.detection_timestamp,
        p.flux,
        p.flux_uncertainty,
        p.vista_id,
        p.plume_latitude_deg,
        p.plume_longitude_deg
      from
        sources as s,
        plumes as p
      where
        p.source_id = s.source_id
      ) as p
        on p.plume_id like concat(f.flight_name, '%%')
          and p.source_id = s.source_id
  left join aviris_plumes as ap
    on ap.candidate_id = p.candidate_id
where
  s.source_id = %s;
                """

        cur.execute(sql,(source_id,))

        results = cur.fetchall()

        cur.close()
        conn.close()

        return results

    @staticmethod
    def __replace_s3_url(url, s3url):
        if url is not None:
            url = url.replace("{s3}/", "{s3}")
            return url.replace("{s3}", s3url)
        else:
            return url

    def __format_results(self, rows, s3url):
        results = []
        for row in rows:
            results.append({
                "total_overflights": row[FlyoverOfSourceColumns.TOTAL_OVERFLIGHTS],
                "plume_detected": row[FlyoverOfSourceColumns.PLUME_DETECTED],
                "q_source_detected": row[FlyoverOfSourceColumns.Q_SOURCE_FINAL],
                "plume_id": row[FlyoverOfSourceColumns.PLUME_ID],
                "plume_date": row[FlyoverOfSourceColumns.PLUME_DATE],
                "vista_id": row[FlyoverOfSourceColumns.VISTA_ID],
                "vista_name": row[FlyoverOfSourceColumns.VISTA_NAME],
                "flightline_date": row[FlyoverOfSourceColumns.FLIGHTLINE_TIMESTAMP],
                "flightline_name": row[FlyoverOfSourceColumns.FLIGHT_NAME],
                "flightline_id": row[FlyoverOfSourceColumns.FLIGHTLINE_ID],
                "source_id": row[FlyoverOfSourceColumns.SOURCE_ID],
                "candidate_id": row[FlyoverOfSourceColumns.CANDIDATE_ID],
                "plume_longitude": row[FlyoverOfSourceColumns.PLUME_LONGITUDE],
                "plume_latitude": row[FlyoverOfSourceColumns.PLUME_LATITUDE],
                "flux": row[FlyoverOfSourceColumns.FLUX],
                "flux_uncertainty": row[FlyoverOfSourceColumns.FLUX_UNCERTAINTY],
                "png_url": self.__replace_s3_url(row[FlyoverOfSourceColumns.PNG_URL], s3url),
                "plume_url": self.__replace_s3_url(row[FlyoverOfSourceColumns.PLUME_URL], s3url),
                "rgbqlctr_url": self.__replace_s3_url(row[FlyoverOfSourceColumns.RGBQLCTR_URL], s3url),
                "png_url_thumb": self.__replace_s3_url(row[FlyoverOfSourceColumns.PNG_URL_THUMB], s3url),
                "plume_url_thumb": self.__replace_s3_url(row[FlyoverOfSourceColumns.PLUME_URL_THUMB], s3url),
                "rgbqlctr_url_thumb": self.__replace_s3_url(row[FlyoverOfSourceColumns.RGBQLCTR_URL_THUMB], s3url)
            })
        return results



    def handle(self, computeOptions, **args):
        source_id = computeOptions.get_argument("source", None)

        rows = self.__query(args["webconfig"], source_id)

        s3url = args["webconfig"].get("s3", "s3.proxyurl")

        results = self.__format_results(rows, s3url)

        return SimpleResult(results)



class FlyoversOfFacilityColumns:
    FACILITY_ID = 0
    FACILITY_CATEGORY_ID = 1
    FACILITY_CATEGORY = 2
    FACILITY_NAME = 3
    FACILITY_OPERATOR = 4
    FACILITY_SITE_NAME = 5
    FACILITY_STATE = 6
    FACILITY_ADDRESS = 7
    FACILITY_SECTOR = 8
    FACILITY_CITY = 9
    FLIGHTLINE_ID = 10
    FLIGHTLINE_DATE = 11
    PLUME_ID = 12
    FLUX = 13
    FLUX_UNCERTAINTY = 14
    CANDIDATE_ID = 15
    PLUME_DATE = 16
    SOURCE_ID = 17
    PLUME_DETECTED = 18
    SECTOR_LEVEL_1 = 19
    SECTOR_LEVEL_2 = 20
    SECTOR_LEVEL_3 = 21
    RGBQLCTR_URL = 22
    RGBQLCTR_URL_THUMB = 23


@service_handler
class FlyoversOfFacilityHandlerImpl(BaseHandler):
    name = "Flyovers of Facility"
    path = "/flyoversOfFacility"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)


    def __query(self, config, vista_id):
        conn = psycopg2.connect(dbname=config.get("database", "db.database"),
                                user=config.get("database", "db.username"),
                                password=config.get("database", "db.password"),
                                host=config.get("database", "db.endpoint"),
                                port=config.get("database", "db.port"))
        cur = conn.cursor()

        sql = """
select distinct
  v.vista_id,
  v.category_id,
  v.category,
  v.name,
  v.operator,
  v.site_name,
  v.state,
  v.address,
  v.sector,
  v.city,
  f.flightline_id,
  to_char(f.flight_timestamp, 'yyyy-mm-dd HH24:MI:SS') as flight_timestamp,
  p.plume_id,
  p.flux,
  p.flux_uncertainty,
  p.candidate_id,
  to_char(p.detection_timestamp, 'yyyy-mm-dd HH24:MI:SS') as detection_timestamp,
  p.source_id,
  p.plume_id is not null as plume_detected,
  v.sector_level_1,
  v.sector_level_2,
  v.sector_level_3,
  ap.rgbqlctr_url,
  ap.rgbqlctr_url_thumb
from
  vista as v
  inner join vista_flightlines vf on v.id = vf.vista_id
  inner join flightlines f on vf.flightline_id = f.flightline_id
  left join plumes as p
      on p.vista_id = v.vista_id
      and p.detection_timestamp = f.flight_timestamp
  left join aviris_plumes as ap
      on ap.candidate_id = p.candidate_id
      and ap.source_id is not null
      and ap.candidate_id is not null
where
  v.vista_id = %s
order by
  flight_timestamp,
  detection_timestamp;    
        """

        cur.execute(sql, (vista_id,))

        results = cur.fetchall()

        cur.close()
        conn.close()

        return results

    @staticmethod
    def __replace_s3_url(url, s3url):
        if url is not None:
            url = url.replace("{s3}/", "{s3}")
            return url.replace("{s3}", s3url)
        else:
            return url

    def __format_results(self, rows, s3url):
        results = []
        for row in rows:
            results.append({
                "facility_id": row[FlyoversOfFacilityColumns.FACILITY_ID],
                "facility_category_id": row[FlyoversOfFacilityColumns.FACILITY_CATEGORY_ID],
                "facility_category": row[FlyoversOfFacilityColumns.FACILITY_CATEGORY],
                "facility_name": row[FlyoversOfFacilityColumns.FACILITY_NAME],
                "facility_operator": row[FlyoversOfFacilityColumns.FACILITY_OPERATOR],
                "facility_site_name": row[FlyoversOfFacilityColumns.FACILITY_SITE_NAME],
                "facility_state": row[FlyoversOfFacilityColumns.FACILITY_STATE],
                "facility_address": row[FlyoversOfFacilityColumns.FACILITY_ADDRESS],
                "facility_sector": row[FlyoversOfFacilityColumns.FACILITY_SECTOR],
                "facility_city": row[FlyoversOfFacilityColumns.FACILITY_CITY],
                "flightline_id": row[FlyoversOfFacilityColumns.FLIGHTLINE_ID],
                "flightline_date": row[FlyoversOfFacilityColumns.FLIGHTLINE_DATE],
                "plume_id": row[FlyoversOfFacilityColumns.PLUME_ID],
                "flux": row[FlyoversOfFacilityColumns.FLUX],
                "flux_uncertainty": row[FlyoversOfFacilityColumns.FLUX_UNCERTAINTY],
                "aviris_plume_id": row[FlyoversOfFacilityColumns.CANDIDATE_ID],
                "candidate_id": row[FlyoversOfFacilityColumns.CANDIDATE_ID],
                "plume_date": row[FlyoversOfFacilityColumns.PLUME_DATE],
                "source_id": row[FlyoversOfFacilityColumns.SOURCE_ID],
                "plume_detected": row[FlyoversOfFacilityColumns.PLUME_DETECTED],
                "sector_level_1": row[FlyoversOfFacilityColumns.SECTOR_LEVEL_1],
                "sector_level_2": row[FlyoversOfFacilityColumns.SECTOR_LEVEL_2],
                "sector_level_3": row[FlyoversOfFacilityColumns.SECTOR_LEVEL_3],
                "rgbqlctr_url": self.__replace_s3_url(row[FlyoversOfFacilityColumns.RGBQLCTR_URL], s3url),
                "rgbqlctr_url_thumb": self.__replace_s3_url(row[FlyoversOfFacilityColumns.RGBQLCTR_URL_THUMB], s3url)
            })
        return results

    def handle(self, computeOptions, **args):
        vista_id = computeOptions.get_argument("vista_id", None)

        rows = self.__query(args["webconfig"], vista_id)
        s3url = args["webconfig"].get("s3", "s3.proxyurl")

        results = self.__format_results(rows, s3url)

        return SimpleResult(results)







def filter_null_results(row, params):
    if row["vista_id"] is None\
                and (      params["sector_level_1"] != "%"
                        or params["sector_level_2"] != "%"
                        or params["sector_level_3"] != "%"
                        or params["vista_category"] != "%"):
        return None
    else:
        return row


def replace_nulls_with_zero_length_string(row, params):
    if row is not None:
        for key in row:
            value = row[key]
            if value is None:
                row[key] = ""
    return row

create_query_based_handler(
    uri="/methanePlumeSources",
    name="Methane Plume Sources",
    sql="""
    select
      s.source_id,
      s.source_latitude_deg,
      s.source_longitude_deg,
      s.source_persistence,
      s.total_overflights,
      s.q_source_final,
      s.q_source_final_sigma,
      v.name as vista_name,
      v.vista_id,
      v.category,
      v.category_id,
      v.sector_level_1 as ipcc_sector_l1,
      v.sector_level_2 as ipcc_sector_l2,
      v.sector_level_3 as ipcc_sector_l3,
      c.coname as county_name,
      s.confidence_in_persistence
    from
      sources as s
      left join vista as v
          on s.vista_id = v.vista_id
              and v.category like %(vista_category)s
              and v.sector_level_1 like %(sector_level_1)s
              and v.sector_level_2 like %(sector_level_2)s
              and v.sector_level_3 like %(sector_level_3)s,
      county_sources as cs,
      counties as c
    where
      cs.source_id = s.source_id
      and c.county_id = cs.county_id
      and c.coname like %(county)s
;
    """,
    params=[
        param("county", default_value="", add_wildcard=True),
        param("vista_category", default_value="", add_wildcard=True),
        param("sector_level_1", default_value="", add_wildcard=True),
        param("sector_level_2", default_value="", add_wildcard=True),
        param("sector_level_3", default_value="", add_wildcard=True)
    ],
    columns=[
        column("source_id", 0),
        column("source_latitude", 1),
        column("source_longitude", 2),
        column("source_persistence", 3),
        column("flyover_count", 4),
        column("q_source_final", 5),
        column("q_source_final_sigma", 6),
        column("vista_name", 7),
        column("vista_id", 8),
        column("vista_category", 9),
        column("vista_category_id", 10),
        column("sector_level_1", 11),
        column("sector_level_2", 12),
        column("sector_level_3", 13),
        column("county_name", 14),
        column("confidence_in_persistence", 15)
    ],
    filters=[
        filter_null_results,
        replace_nulls_with_zero_length_string
    ]
)



create_query_based_handler(
    uri="/methanePlumeSourcesSummary",
    name="Methane Plume Sources Summary",
    sql="""
    select
      s.source_id,
      s.source_persistence,
      s.total_overflights,
      s.q_source_final,
      s.q_source_final_sigma,
      s.confidence_in_persistence,
      v.vista_id
    from
      sources as s
      left join vista as v
          on s.vista_id = v.vista_id
              and v.category like %(vista_category)s
              and v.sector_level_1 like %(sector_level_1)s
              and v.sector_level_2 like %(sector_level_2)s
              and v.sector_level_3 like %(sector_level_3)s,
      county_sources as cs,
      counties as c
    where
      cs.source_id = s.source_id
      and c.county_id = cs.county_id
      and c.coname like %(county)s
;
    """,
    params=[
        param("county", default_value="", add_wildcard=True),
        param("vista_category", default_value="", add_wildcard=True),
        param("sector_level_1", default_value="", add_wildcard=True),
        param("sector_level_2", default_value="", add_wildcard=True),
        param("sector_level_3", default_value="", add_wildcard=True)
    ],
    columns=[
        column("number_of_sources", 0),
        column("avg_source_persistance", 1),
        column("total_overflights", 2),
        column("avg_q_source_final", 3),
        column("avg_q_source_final_sigma", 4),
        column("avg_confidence_in_persistence", 5),
        column("vista_id", 6)
    ],
    filters=[
        filter_null_results
    ],
    summarize=[
        summary("number_of_sources", SummaryTypes.COUNT_DISTINCT),
        summary("avg_source_persistance", SummaryTypes.AVERAGE),
        summary("total_overflights", SummaryTypes.SUM),
        summary("avg_q_source_final", SummaryTypes.AVERAGE),
        summary("avg_q_source_final_sigma", SummaryTypes.AVERAGE),
        summary("avg_confidence_in_persistence", SummaryTypes.AVERAGE)
    ]
)
















class EmissionsBySourceColumns:
    SOURCE_ID = 0
    SECTORS_LEVEL_1 = 1
    SECTORS_LEVEL_2 = 2
    SECTORS_LEVEL_3 = 3
    NEAREST_FACILITY = 2
    NUMBER_OF_PLUMES = 3
    AVG_IME20_1500PPMM_150M = 4
    MIN_IME20_1500PPMM_150M = 5
    MAX_IME20_1500PPMM_150M = 6
    AVG_FLUX = 7
    MIN_FLUX = 8
    MAX_FLUX = 9

@service_handler
class EmissionsBySourceHandlerImpl(BaseHandler):
    name = "Emissions by Source"
    path = "/emissionsBySource"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)


    def __query(self, config, county=None, sector=None, subsector=None, from_date=None, to_date=None):
        county = "" if county is None else county
        sector = "" if sector is None else sector
        subsector = "" if subsector is None else subsector
        to_date = "now()" if to_date is None else self.__format_dt(to_date)
        from_date = "1970-01-01" if from_date is None else self.__format_dt(from_date)

        conn = psycopg2.connect(dbname=config.get("database", "db.database"),
                                user=config.get("database", "db.username"),
                                password=config.get("database", "db.password"),
                                host=config.get("database", "db.endpoint"),
                                port=config.get("database", "db.port"))
        cur = conn.cursor()

        sql = """
select distinct
  s.source_id,
  s.sector_level_1,
  s.sector_level_2,
  s.sector_level_3,
  s.nearest_facility,
  count(p.plume_id) as plume_count,
  avg(p.ime20_1500ppmm_150m) as avg_ime20_1500ppmm_150m,
  min(p.ime20_1500ppmm_150m) as min_ime20_1500ppmm_150m,
  max(p.ime20_1500ppmm_150m) as max_ime20_1500ppmm_150m,
  avg(p.flux) as avg_flux,
  min(p.flux) as min_flux,
  max(p.flux) as max_flux
from
  counties as c,
  county_sources as cs,
  sources as s
  left join plumes as p
      on p.source_id = s.source_id
      and p.detection_timestamp between %s and %s
where
  c.county_id = cs.county_id
  and cs.source_id = s.source_id
  and c.name like %s
  and s.sector_level_1 like %s
  and s.sector_level_2 like %s
group by
  s.source_id,
  s.sector_level_1,
  s.sector_level_2,
  s.sector_level_3,
  s.nearest_facility;
        """

        cur.execute(sql,
                    (
                        from_date,
                        to_date,
                        "%{county}%".format(county=county),
                        "%{sector}%".format(sector=sector),
                        "%{subsector}%".format(subsector=subsector)
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
                "source_id": row[EmissionsBySourceColumns.SOURCE_ID],
                "sectors_level_1": row[EmissionsBySourceColumns.SECTORS_LEVEL_1],
                "sectors_level_2": row[EmissionsBySourceColumns.SECTORS_LEVEL_2],
                "sectors_level_3": row[EmissionsBySourceColumns.SECTORS_LEVEL_3],
                "nearest_facility": row[EmissionsBySourceColumns.NEAREST_FACILITY],
                "number_of_plumes": row[EmissionsBySourceColumns.NUMBER_OF_PLUMES],
                "avg_ime20_1500ppmm_150m": row[EmissionsBySourceColumns.AVG_IME20_1500PPMM_150M],
                "min_ime20_1500ppmm_150m": row[EmissionsBySourceColumns.MIN_IME20_1500PPMM_150M],
                "max_ime20_1500ppmm_150m": row[EmissionsBySourceColumns.MAX_IME20_1500PPMM_150M],
                "avg_flux": row[EmissionsBySourceColumns.AVG_FLUX],
                "min_flux": row[EmissionsBySourceColumns.MIN_FLUX],
                "max_flux": row[EmissionsBySourceColumns.MAX_FLUX]
            })
        return results

    def handle(self, computeOptions, **args):
        county = computeOptions.get_argument("county", None)
        sector = computeOptions.get_argument("sector", None)
        subsector = computeOptions.get_argument("subsector", None)
        from_date = computeOptions.get_datetime_arg("from_date", None)
        to_date = computeOptions.get_datetime_arg("to_date", None)
        rows = self.__query(args["webconfig"], county, sector, subsector, from_date, to_date)
        results = self.__format_results(rows)
        return SimpleResult(results)