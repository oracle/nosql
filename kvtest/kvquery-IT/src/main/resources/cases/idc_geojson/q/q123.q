#geo_is_geometry():operand returns in-valid GeoJson object. 
# Invalid "type": "MultiLineStrings"

select id, geo_is_geometry({
		"kind": "railtrack",
		"country": "india",
		"city": "bengaluru",
		"geom": {
			"type": "MultiLineStrings",
			"coordinates": [
				[
					[
						77.59585618972778,
						12.994762468222667
					],
					[
						77.59606003761292,
						12.994710197830848
					],
					[
						77.59631752967834,
						12.994542932503089
					],
					[
						77.59731531143188,
						12.994218855609887
					],
					[
						77.59783029556274,
						12.994020227627564
					]
				],
				[
					[
						77.59907484054565,
						12.993842507719203
					],
					[
						77.59933233261108,
						12.993528884041217
					],
					[
						77.59960055351257,
						12.993340709644286
					],
					[
						77.60051250457764,
						12.99303753948261
					]
				]
			]
		}
	}
) 
from geotypes g
where id =1

