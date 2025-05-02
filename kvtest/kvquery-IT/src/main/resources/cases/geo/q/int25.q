#
# the search geometry crosses the antimeridian, but the index is not used
#
select /*+ FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [ 
                                          [ 178.36, -17.20],
                                          [-179.62, -17.20],
                                          [-179.58, -15.89],
                                          [ 177.83, -15.74],
                                          [ 178.36, -17.20]
                                      ] ]
                    } )
