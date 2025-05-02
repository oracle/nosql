#
# The Greenwich fat-line
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [0,      -90],
                                          [0.0001, -90],
                                          [0.0001,  90],
                                          [0,       90],
                                          [0,      -90]
                                      ] ]
                    } )
