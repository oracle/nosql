# operand1 is Point and operand2 is Point, both geometries have a point in common

select id, p.info.point
 from points p
 where geo_intersect(p.info.point,
                   {
        "type": "Point",
        "coordinates": [
          77.55,
          8.07
        ]
      })

