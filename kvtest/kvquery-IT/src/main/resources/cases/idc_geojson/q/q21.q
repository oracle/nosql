#operand1 is Point and operand2 is MultiPloygon, both geometries don't have any point in common

select id, p.info.point
 from points p
 where geo_intersect(p.info.point,
               {
"type": "MultiPolygon",
"coordinates": [
[
[
[102.0, 2.0],
[103.0, 2.0],
[103.0, 3.0],
[102.0, 3.0],
[102.0, 2.0]
]
],
[
[
[100.0, 0.0],
[101.0, 0.0],
[101.0, 1.0],
[100.0, 1.0],
[100.0, 0.0]
]
]
]
})

