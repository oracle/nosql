# operand1 is Point and operand2 is Polygon, 1st geometry is not completely contained inside 2nd geometry

select geo_inside(p.info.geom, {
	"type": "Polygon",
	"coordinates": [
		[
			[
				-52.734375,
				-12.64033830684679
			],
			[
				-46.669921875,
				-7.928674801364035
			],
			[
				-52.9541015625,
				-7.01366792756663
			],
			[
				-52.734375,
				-12.64033830684679
			]
		]
	]
})
from geotypes p where id = 2