#
# Raises error, because the last coord is not the same as the 1st:
# java.lang.IllegalArgumentException: Error: at (7, 27) The right operand
# of geo_intersect function  is not a geometry.
#
select /*+ FORCE_PRIMARY_INDEX(points) */ *
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [ [24.00, 35.50],
                                          [24.02, 35.50],
                                          [24.02, 35.52],
                                          [24.00, 35.52] ] ]
                    } )
