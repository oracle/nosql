select /* FORCE_PRIMARY_INDEX(netflix) */ acct_id, user_id
from netflix n, unnest(n.value.contentStreamed[] as $shows)
where $shows.showId = 15 and $shows.seriesInfo[].episodes[].minWatched >any 40
