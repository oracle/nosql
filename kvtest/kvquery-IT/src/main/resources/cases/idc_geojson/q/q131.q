# operand1 is Point and operand2 is Polygon, 1st geometry is completely contained inside 2nd geometry

select geo_inside(p.info.geom,    {
	"type": "Polygon",
	"coordinates": [
		[
			[77.63795614242554, 12.962012913233123],
			[77.6381117105484,  12.960810533060219],
			[77.63918995857237, 12.959885219146908],
			[77.64020919799805, 12.960094329936581],
			[77.64045596122742, 12.961469229004685],
			[77.64047741889954, 12.962211566789625],
			[77.63795614242554, 12.962012913233123]
		]
	]
})from geotypes p where id = 2