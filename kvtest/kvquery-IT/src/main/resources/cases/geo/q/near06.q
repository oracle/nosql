#
# All points within 5km from chania city center
#
select /*+ FORCE_INDEX(points idx_kind_ptn) */
       id, p.info.point
from points p
where geo_near(p.info.point, 
               { "type" : "point", "coordinates" : [ 24.0175, 35.5156 ] },
               5000)
