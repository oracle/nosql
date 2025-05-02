select /*+ FORCE_INDEX(viewers idx2_country_showid_date) */ 
    $show.showId, count(*) as cnt
from viewers v, UNNEST(v.info.shows[] as $show)
where v.info.country = "USA"
group by $show.showId
order by count(*) desc
