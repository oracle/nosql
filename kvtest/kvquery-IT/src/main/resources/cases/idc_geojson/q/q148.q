#operand1 is MultiPolygon and operand2 is non Polygon geometry

select geo_inside(p.info.geom,{ "type" : "point", "coordinates" : [ 77.58909702301025, 12.97269293084298 ] })from geotypes p where id = 7