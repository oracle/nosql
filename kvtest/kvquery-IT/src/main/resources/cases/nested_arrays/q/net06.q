select $shows.showId,
       sum(n.value.contentStreamed[$element.showId = $shows.showId].
           seriesInfo.episodes.minWatched) as total_time
from netflix n, unnest(n.value.contentStreamed[] as $shows)
group by $shows.showId
order by sum(n.value.contentStreamed[$element.showId = $shows.showId].
             seriesInfo.episodes.minWatched)
