#operand1 is MultiLineString and operand2 is Polygon, 1st geometry is completely contained inside 2nd geometry

select geo_inside(p.info.geom,
{
	"type": "Polygon",
	"coordinates": [
		[
			[77.59536266326904, 12.995734695504481],
			[77.59433269500732, 12.991762129920927],
			[77.59931087493896, 12.989964000395487],
			[77.60561943054199, 12.991929397121895],
			[77.60145664215088, 12.995734695504481],
			[77.59536266326904, 12.995734695504481]
		]
	]
})from geotypes p where id = 5