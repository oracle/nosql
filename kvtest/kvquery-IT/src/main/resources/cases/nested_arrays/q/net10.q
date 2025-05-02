select /* FORCE_PRIMARY_INDEX(netflix) */
       $show.showId,
       $seriesInfo.seasonNum,
       sum(n.value.contentStreamed[$element.showId = $show.showId].
           seriesInfo[$element.seasonNum = $seriesInfo.seasonNum].
           episodes.minWatched) as length
from netflix n, unnest(n.value.contentStreamed[] as $show,
                       $show.seriesInfo[] as $seriesInfo)
group by $show.showId, $seriesInfo.seasonNum
order by sum(n.value.contentStreamed[$element.showId = $show.showId].
           seriesInfo[$element.seasonNum = $seriesInfo.seasonNum].
           episodes.minWatched)
