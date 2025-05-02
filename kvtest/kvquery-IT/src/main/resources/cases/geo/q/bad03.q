#
# JGeometry asJgeom(String jsonGeom) thows:
#
# java.lang.IllegalArgumentException: No geometry field: ()
#
select *
from points
where geo_intersect(point, [1, 2])

