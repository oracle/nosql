select $show.showId,
       $season.seasonNum,
       sum($season.episodes.minWatched) as totalTime
from viewers v, UNNEST(v.info.shows[] as $show,
                       $show.seasons[] as $season)
where v.info.country = "USA"
group by $show.showId, $season.seasonNum
order by sum($season.episodes.minWatched) desc
