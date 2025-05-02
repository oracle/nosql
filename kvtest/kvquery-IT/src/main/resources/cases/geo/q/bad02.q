#
# JGeometry asJgeom(String jsonGeom) thows:
#
# java.lang.IllegalArgumentException: EMPTY geometry
#
select *
from points p
where geo_intersect(p.info.point, 
                    { "type" : "foo",
                      "coordinates" : [ [-12, 3], [-9, 3], [-9, 7], [-12, 7], [-12, 3] ]
                    } )
