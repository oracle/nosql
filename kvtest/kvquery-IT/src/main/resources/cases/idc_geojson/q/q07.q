# Distance from Kanyakumari to Australia:9026 km

select id,
       p.info.point,
       geo_distance(p.info.point,
                   { "type" : "LineString", "coordinates": [[147.27705001831055, -43.45404042552573], [147.28031158447266, -43.46164137177049]] }) as dist
from points p
where id = 10
