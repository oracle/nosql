select $shows.showId, $shows.showName, count(*) as cnt
from netflix n, unnest(n.value.contentStreamed[] as $shows)
group by $shows.showId, $shows.showName
order by count(*) desc
