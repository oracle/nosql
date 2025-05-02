#operand1 is LineString and operand2 is non Polygon geometry

select geo_inside(p.info.geom, {
	"type": "GeometryCollection",
	"geometries": [{
		"type": "Point",
		"coordinates": [100.0, 0.0]
	}, {
		"type": "LineString",
		"coordinates": [
			[101.0, 0.0],
			[102.0, 1.0]
		]
	}]
})from geotypes p where id = 4