#
# Distance between los gatos and paris
#
select geo_distance(p.info.point, 
                    { "type" : "point", "coordinates" : [ 2.75, 48.44 ]})
from points p
where id = 512
