select $show.showId, count(*) as cnt
from viewers v, UNNEST(v.shows[] as $show)
where v.country = "USA"
group by $show.showId
order by count(*) desc
