#operand1 is LineString and operand2 is Polygon, 1st geometry is not completely contained inside 2nd geometry

select geo_inside(p.info.geom,   {
	"type": "Polygon",
	"coordinates": [
		[
			[77.59680032730103, 12.99963402042749],
			[77.59599566459656, 12.998975425611215],
			[77.59617805480957, 12.997783678168052],
			[77.59796977043152, 12.997982303139281],
			[77.59742259979248, 12.999519027807716],
			[77.59680032730103, 12.99963402042749]
		]
	]
})from geotypes p where id = 4