select /*+ FORCE_INDEX(viewers idx_country) */ 
    $show.showId, sum($show.seasons[].episodes[].minWatched) as time
from viewers v, UNNEST(v.info.shows[] as $show)
where v.info.country = "USA"
group by $show.showId
order by sum($show.seasons[].episodes[].minWatched) desc
