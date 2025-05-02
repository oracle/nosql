#
# Distance between a point and a polygon that stradles the antimeridian
#
select geo_distance(p.info.point,
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [ 178.614008, -27.238157],
                                          [-179.529303, -26.270334],
                                          [ 179.899408, -25.848031],
                                          [ 178.240473, -26.565408],
                                          [ 178.614008, -27.238157]
                               ] ] }
)
from points p
where id = 12
