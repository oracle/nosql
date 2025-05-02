select  count(*)
from viewers v
where exists v.info.shows[$element.showId = 16].
             seasons.episodes[substring($element.date, 0, 4) = "2021"]
