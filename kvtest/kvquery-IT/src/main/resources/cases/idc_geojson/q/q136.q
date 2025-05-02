#operand1 is MultiPoint and operand2 is non Polygon geometry

select geo_inside(p.info.geom,
{"type": "point", "coordinates": [77.63939380645752, 12.961401268392635]}) 
from geotypes p where id = 3