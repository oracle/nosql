# Distance from Hamilton, New Zealand and Cordoba, Spain: 20,000 km

select id,
       p.info.point,
       cast(geo_distance(p.info.point,
                         {
                           "type" : "point", 
                           "coordinates" : [ -4.77604866027832, 37.884913694932365] 
                         }) * 1000000 
            as long) as dist
from points p
where id = 6
