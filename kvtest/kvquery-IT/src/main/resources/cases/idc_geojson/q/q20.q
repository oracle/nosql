#operand1 is Point and operand2 is Ploygon, both geometries don't have any point in common

select id, p.info.point
 from points p
 where geo_intersect(p.info.point,
                 {
        "type": "Polygon",
        "coordinates": [
          [
            [
              75.25634765625,
              31.203404950917395
            ],
            [
              76.1187744140625,
              31.203404950917395
            ],
            [
              76.1187744140625,
              31.39115752282472
            ],
            [
              75.25634765625,
              31.39115752282472
            ],
            [
              75.25634765625,
              31.203404950917395
            ]
          ]
        ]
      })
