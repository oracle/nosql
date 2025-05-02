#operand1 is MultiLineString and operand2 is Polygon, 1st geometry is not completely contained inside 2nd geometry

select geo_inside(p.info.geom,
{
	"type": "Polygon",
	"coordinates": [
		[
			[77.58673667907713, 12.987935861936778],
			[77.59004116058348, 12.986033160287572],
			[77.5925087928772, 12.990005817509065],
			[77.58793830871582, 12.989796731870824],
			[77.58673667907713, 12.987935861936778]
		]
	]
}
)from geotypes p where id = 5