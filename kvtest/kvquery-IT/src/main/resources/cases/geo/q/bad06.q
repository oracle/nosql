#
# JGeometry asJgeom(String jsonGeom) thows:
#
# java.lang.IllegalArgumentException: Unexpected json data: "boo"
#
select *
from points p
where geo_intersect(p.info.point,
                    { "type" : "polygon",
                      "coordinates" : "boo"
                    } )
