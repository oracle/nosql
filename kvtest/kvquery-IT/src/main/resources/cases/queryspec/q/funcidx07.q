select 
/*+ force_index(viewers idx_country_showid_date) */
acct_id, user_id
from Viewers v
where substring(v.info.shows.seasons.episodes.date, 0, 4) =any "2021"
