select /*+ FORCE_INDEX(netflix idx_showid_minWatched) */
       $shows.showId, count(*) as cnt
from netflix n, unnest(n.value.contentStreamed[] as $shows)
group by $shows.showId
order by count(*)
