#
# Should raise error, because it contains an invalid latidute,
# but it doesn't.
#
select /* FORCE_PRIMARY_INDEX(points) */ *
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [ [ 24, 35],
                                          [250, 35],
                                          [ 25, 36],
                                          [ 24, 36],
                                          [24, 35] ] ]
                    } )
