select count(*) as cnt
from viewers v
where v.country = "USA" and 
      exists v.shows[
          exists $element.genres[$element in ("french", "danish")] and
          exists $element.seasons.episodes["2021-01-01" <= $element.date and
                                           $element.date <= "2021-12-31"]
      ]
