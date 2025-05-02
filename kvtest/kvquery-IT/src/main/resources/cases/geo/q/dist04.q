#
# Distance between india and los gatos
#
select geo_distance({ "type" : "point", "coordinates" : [ 75.08, 24.44 ]},
                    p.info.point)
from points p
where id = 512
