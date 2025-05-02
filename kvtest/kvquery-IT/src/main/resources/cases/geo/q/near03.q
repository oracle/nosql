#
# Nearest neighbors within 5km from chania city center
#
select /* FORCE_PRIMARY_INDEX(points) */
       id,
       p.info.point,
       geo_distance(p.info.point,
                    { "type" : "point", "coordinates" : [ 24.0175, 35.5156 ] }) as dist
from points p
where geo_near(p.info.point, 
               { "type" : "point", "coordinates" : [ 24.0175, 35.5156 ] },
               5000)
