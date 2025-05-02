#
# Distance between 2 points that stradle the antimeridian
#
select geo_distance(p.info.point, 
                    { "type" : "point", "coordinates" : [-179.127, -16.8 ] })
from points p
where id = 12
