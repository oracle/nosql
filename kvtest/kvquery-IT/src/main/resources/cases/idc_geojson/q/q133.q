# operand1 is Point and operand2 is non Polygon geometry

select geo_inside(p.info.geom,
{
	"type": "point",
	"coordinates": [79.85367000102997, 6.934451956982983]
})from geotypes p where id = 2