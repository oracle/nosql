#
# JGeometry asJgeom(String jsonGeom) thows:
#
# java.lang.IllegalArgumentException: No geometry field: (coords,type)
#
select *
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coords" : [ [-12, 3], [-9, 3], [-9, 7], [-12, 7], [-12, 3] ]
                    } )
