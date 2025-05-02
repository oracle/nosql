select /* FORCE_PRIMARY_INDEX(netflix) */
       $shows.showId, sum($shows.seriesInfo.episodes.minWatched) as total_time
from netflix n, unnest(n.value.contentStreamed[] as $shows)
group by $shows.showId
order by sum($shows.seriesInfo.episodes.minWatched)
