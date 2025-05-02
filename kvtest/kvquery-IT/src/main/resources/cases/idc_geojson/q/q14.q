#Intersect with huge box having most of the points using Polygon.

select id, p.info.point
 from points p
 where geo_intersect(p.info.point,
                    {
        "type": "Polygon",
        "coordinates": [
          [
            [
              74.44335937499999,
              10.703791711680736
            ],
            [
              78.44238281249999,
              10.703791711680736
            ],
            [
              78.44238281249999,
              13.966054081318314
            ],
            [
              74.44335937499999,
              13.966054081318314
            ],
            [
              74.44335937499999,
              10.703791711680736
            ]
          ]
        ]
      })
order by id