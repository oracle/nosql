#operand1 is Point and operand2 is Point, both geometries don't have any point in common

select id, p.info.point
 from points p
 where geo_intersect(p.info.point,
                  {
        "type": "Point",
        "coordinates": [
          77.55755424499512,
          8.105542704535111
        ]
      })

