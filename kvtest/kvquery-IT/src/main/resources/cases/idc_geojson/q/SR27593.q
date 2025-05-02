#
# When the search geometry crosses the antimeridian, the index should not be used.
#

select /*+ FORCE_PRIMARY_INDEX(points) */
id, geo_distance(p.info.point,
{
  "type": "Polygon",
  "coordinates": [
      [
          [
              -189.31640625,
              68.46379955520322
          ],
          [
              -195.46875,
              50.736455137010665
          ],
          [
              -147.83203125,
              51.83577752045248
          ],
          [
              -142.91015625,
              68.2042121888185
          ],
          [
              -189.31640625,
              68.46379955520322
          ]
      ]
  ]
})
from points p