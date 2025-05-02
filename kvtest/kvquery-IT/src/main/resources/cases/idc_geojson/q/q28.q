#operand1 is Point and operand2 is MultiPoint

select id,
       p.info.point,
       cast(geo_distance(p.info.point,
                         {
                           "type": "multipoint", 
                           "coordinates": [
                             [79.85287606716155, 6.934201673601963],
                             [79.85367000102997, 6.934451956982983]
                           ]
                         }) * 1000000 
            as long) as dist
from points p
order by id
