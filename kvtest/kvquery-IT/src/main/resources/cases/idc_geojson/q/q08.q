# Distance from Greenwich to Jhansi:7,045km

select id,
       p.info.point,
       geo_distance(p.info.point,
                   {
        "type": "Polygon",
        "coordinates": [
          [
            [
              78.57638597488403,
              25.450773090290603
            ],
            [
              78.57827425003052,
              25.450773090290603
            ],
            [
              78.57827425003052,
              25.451993728554815
            ],
            [
              78.57638597488403,
              25.451993728554815
            ],
            [
              78.57638597488403,
              25.450773090290603
            ]
          ]
        ]
      }) as dist
from points p
where id = 11
