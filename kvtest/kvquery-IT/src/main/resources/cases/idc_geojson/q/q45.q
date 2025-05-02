#
# operand1 is Point and operand2 is LineString, 1st geometry is within distance specified by operand3 from 2nd geometry
# Should return true.
#
select geo_within_distance(p.info.point,
                         {
        "type": "LineString",
        "coordinates": [
          [
            77.57508516311646,
            12.972943850854447
          ],
          [
            77.57585763931274,
            12.971877439057483
          ],
          [
            77.57607221603394,
            12.970957393833265
          ],
          [
            77.57607221603394,
            12.970246447465623
          ],
          [
            77.57686614990234,
            12.969012741593874
          ],
          [
            77.57871150970459,
            12.969305485913676
          ],
          [
            77.58057832717896,
            12.970058255439973
          ],
          [
            77.5820803642273,
            12.969075472548546
          ]
        ]
      },
                          300000.098)
from points p
where id =18