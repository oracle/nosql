#
# Horizontal line across chania city
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "LineString",
                      "coordinates" : [ [24.013270, 35.5158], [24.019193, 35.5158] ]
                    } )
