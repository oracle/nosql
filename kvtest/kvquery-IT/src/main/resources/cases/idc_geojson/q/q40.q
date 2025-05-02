select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_within_distance(p.info.point,
                          { "type" : "point", "coordinates" : [  -4.77604866027832, 37.884913694932365 ] },
                          19982000000.1)