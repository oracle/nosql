select $shows.showId, count(*) as cnt
from netflix n, unnest(n.value.contentStreamed[] as $shows)
group by $shows.showId
order by count(*) desc
