select /*+ FORCE_INDEX(viewers idx2_country_showid_date) */ 
    $show.showId, sum($show.seasons[].episodes[].minWatched) as time
from viewers v, UNNEST(v.info.shows[] as $show)
where v.info.country = "USA"
group by $show.showId
order by sum($show.seasons[].episodes[].minWatched) desc
