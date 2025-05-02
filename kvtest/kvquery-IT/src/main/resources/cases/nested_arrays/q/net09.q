select /* FORCE_PRIMARY_INDEX(netflix) */
       $show.showId,
       $seriesInfo.seasonNum,
       sum($seriesInfo.episodes.minWatched) as length
from netflix n, unnest(n.value.contentStreamed[] as $show,
                       $show.seriesInfo[] as $seriesInfo)
group by $show.showId, $seriesInfo.seasonNum
order by sum($seriesInfo.episodes.minWatched)
