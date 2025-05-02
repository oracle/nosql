#
# All points within 100km from chania city center
#
declare $p2x double;
        $point string;
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_within_distance(p.info.point, 
                          { "type" : $point, "coordinates" : [ $p2x, 35.5156 ] },
                          100000)
