#operand1 is MultiLineString and operand2 is non Polygon geometry

select geo_inside(p.info.geom,
{
	"type": "MultiPoint",
	"coordinates": [
		[121.9447, 37.2975],
		[121.9500, 37.3171],
		[122.3975, 37.6144]
	]
})from geotypes p where id = 5