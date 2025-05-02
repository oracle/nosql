select $show.showId,
       sum($show.seasons.episodes.minWatched) as total_time
from viewers $v, UNNEST($v.shows[] as $show)
where $v.country = "USA"
group by $show.showId
order by sum($show.seasons.episodes.minWatched)
