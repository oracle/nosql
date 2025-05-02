#
# All points within 5km from chania or paleochora city center
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, 
       geo_distance(p.info.point,
                    { "type" : "multipoint",
                      "coordinates" : [ [24.0175, 35.5156], [23.6793, 35.2203 ] ] })
from points p
where geo_near(p.info.point, 
               { "type" : "multipoint",
                 "coordinates" : [ [24.0175, 35.5156], [23.6793, 35.2203 ] ] },
                  5000)
