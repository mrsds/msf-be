
from msfbe.queryhandlers import *


create_query_based_handler(
    uri="/list/counties",
    name="California Counties",
    sql="select county_id, name, area, perimeter from counties where name like %(name)s order by name;",
    params=[
        param("name", default_value="", add_wildcard=True)
    ],
    columns=[
        column("county_id", 0),
        column("name", 1),
        column("area", 2),
        column("perimeter", 3)
    ]
)


create_query_based_handler(
    uri="/list/sectors",
    name="IPCCC Sectors",
    sql="select distinct s.sector_level_1, s.sector_level_2, s.sector_level_3 from vista as s where s.sector_level_1 is not null order by s.sector_level_1, s.sector_level_2, s.sector_level_3;",
    params=[

    ],
    columns=[
        column("sector_level_1", 0),
        column("sector_level_2", 1),
        column("sector_level_3", 2)
    ]
)

create_query_based_handler(
    uri="/plumesDateRange",
    name="Min/Max Dates for Plume Detections",
    sql="select to_char(min(p.detection_timestamp), 'yyyy-mm-dd HH24:MI:SS') as min_date, to_char(max(p.detection_timestamp), 'yyyy-mm-dd HH24:MI:SS') as max_date from plumes as p;",
    params=[

    ],
    columns=[
        column("min_date", 0),
        column("max_date", 1)
    ]
)

create_query_based_handler(
    uri="/list/categories",
    name="Vista Categories",
    sql="""select distinct
  v.category_id,
  v.category
from
  vista as v
order by
  v.category;""",
    params=[

    ],
    columns=[
        column("category_id", 0),
        column("category", 1)
    ]
)