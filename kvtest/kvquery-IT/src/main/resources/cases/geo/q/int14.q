#
# The eastern hemisphere
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [0,   -89],
                                          [179, -89],
                                          [179,  89],
                                          [0,    89],
                                          [0,   -89]
                                      ] ]
                    } )
