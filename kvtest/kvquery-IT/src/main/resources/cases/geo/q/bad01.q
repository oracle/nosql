#
# JGeometry asJgeom(String jsonGeom) thows:
#
# java.lang.IllegalArgumentException: Unexpected json data: 35
#
select *
from points p
where geo_intersect(p.info.point,
                    { "type" : "polygon",
                      "coordinates" : 35
                    } )
