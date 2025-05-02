#
# JGeometry asJgeom(String jsonGeom) thows:
#
# java.lang.IllegalArgumentException: Simple polygon must consist of an array of segments
#
select *
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : []
                    } )
