#
# All hotels in chania county
#
declare $kind2 string;
        $radius2 double;
        $p3x double;
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where p.info.kind = $kind2 and
      geo_within_distance(p.info.point, 
                          { "type" : "point",
                            "coordinates" : [$p3x, 35.39]
                          },
                          $radius2)
