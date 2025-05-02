select /*+ FORCE_INDEX(viewers idx6_country_showid_seasonnum_minWatched) */ 
    $show.showId, count(*) as cnt
from viewers v, UNNEST(v.info.shows[] as $show)
where v.info.country = "USA"
group by $show.showId
order by count(*) desc
