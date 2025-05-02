select count(*) as cnt
from viewers v
where v.country = "USA" and
      exists v.shows[$element.showId = 18]
