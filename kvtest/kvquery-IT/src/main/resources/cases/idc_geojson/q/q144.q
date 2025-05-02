#operand1 is Polygon and operand2 is Polygon, 1st geometry is not completely contained inside 2nd geometry

 select geo_inside(p.info.geom,
 {
	"type": "Polygon",
	"coordinates": [
		[
			[-114.03836488723755, 51.04793016540857],
			[-114.0407145023346, 51.04774130787994],
			[-114.0405535697937, 51.04722869070621],
			[-114.03973817825317, 51.04639230313812],
			[-114.03811812400818, 51.046466499419935],
			[-114.03819322586058, 51.04691841966079],
			[-114.03836488723755, 51.04793016540857]
		]
	]
})from geotypes p where id = 6