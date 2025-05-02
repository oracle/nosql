select count(*) as cnt
from viewers v
where v.country = "USA" and
      exists v.shows[$element.showId = 16].
             seasons.episodes[$element.date > "2021-04-01"]
