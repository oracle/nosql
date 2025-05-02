#
# Distance between los gatos and portland ME
#
select geo_distance(p.info.point, 
                    { "type" : "point", "coordinates" : [ -98.84, 44.24 ]})
from points p
where id = 512
