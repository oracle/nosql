#geo_is_geometry():operand returns in-valid GeoJson object. 
# Invalid "type": "Polygons"

select id, geo_is_geometry({
			"type": "Polygons",
			"coordinates": [
				[
					[
						-114.0410578250885,
						51.04741080535188
					],
					[
						-114.04072523117065,
						51.04645975430831
					],
					[
						-114.03851509094237,
						51.046567675976306
					],
					[
						-114.03748512268066,
						51.04708030046554
					],
					[
						-114.03986692428587,
						51.04743104026829
					],
					[
						-114.0410578250885,
						51.04741080535188
					]
				]
			]
		}
) 
from geotypes g
where id =1

