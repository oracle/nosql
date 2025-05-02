select /* FORCE_INDEX(viewers idx_country_showid) */
       $show.showId,
       sum(v.info.shows[$element.showId = $show.showId].
           seasons.episodes.minWatched) as total_time
from viewers v, unnest(v.info.shows[] as $show)
where v.info.country = "USA"
group by $show.showId
order by sum(v.info.shows[$element.showId = $show.showId].
             seasons.episodes.minWatched)
