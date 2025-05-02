#
# Distance between paris and los gatos
#
select geo_distance({ "type" : "point", "coordinates" : [ 2.75, 48.44 ]},
                    p.info.point)
from points p
where id = 512
