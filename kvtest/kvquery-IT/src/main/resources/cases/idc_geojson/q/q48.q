#
#operand1 is Point and operand2 is Polygon, 1st geometry is not within distance specified by operand3 from 2nd geometry
# Should return false
#
select geo_within_distance(p.info.point,
                         {
        "type": "Polygon",
        "coordinates": [
          [
            [
              77.49481201171875,
              12.958724651072012
            ],
            [
              77.48331069946289,
              12.944002741634725
            ],
            [
              77.49910354614258,
              12.938649104401463
            ],
            [
              77.5224494934082,
              12.94316624339459
            ],
            [
              77.5521469116211,
              12.954207794100693
            ],
            [
              77.56811141967773,
              12.973445690117916
            ],
            [
              77.56158828735352,
              12.989671280403403
            ],
            [
              77.50133514404297,
              12.990173085892906
            ],
            [
              77.49481201171875,
              12.958724651072012
            ]
          ]
        ]
      },
                          200000.098)
from points p
where id =18