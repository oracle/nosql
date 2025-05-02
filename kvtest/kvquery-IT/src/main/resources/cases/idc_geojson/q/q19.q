#operand1 is Point and operand2 is Ploygon, both geometries have a point in common

select id, p.info.point
 from points p
 where geo_intersect(p.info.point,
                 {
        "type": "Polygon",
        "coordinates": [
          [
            [
              77.21844255924225,
              28.63192047189977
            ],
            [
              77.2210630774498,
              28.63192047189977
            ],
            [
              77.2210630774498,
              28.633768516895085
            ],
            [
              77.21844255924225,
              28.633768516895085
            ],
            [
              77.21844255924225,
              28.63192047189977
            ]
          ]
        ]
      })

