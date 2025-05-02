#operand1 is Point and operand2 is LineString, both geometries don't have any point in common

select id, p.info.point
 from points p
 where geo_intersect(p.info.point,
                 {
        "type": "LineString",
        "coordinates": [
          [
            80.9857177734375,
            16.688816956180833
          ],
          [
            80.9857177734375,
            16.573022719182777
          ],
          [
            80.9857177734375,
            16.404470456702423
          ],
          [
            80.9857177734375,
            16.161920953785344
          ]
        ]
      })

