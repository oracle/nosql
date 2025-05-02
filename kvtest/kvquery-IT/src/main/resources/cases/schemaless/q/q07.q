select acct_id, user_id,
       $show.showName, $season.seasonNum, $episode.episodeID, $episode.date
from viewers v, v.shows[] as $show,
                $show.seasons[] as $season,
                $season.episodes[] as $episode
where v.country = "USA" and
      $show.showId = 16 and
      $show.seasons.episodes.date >any "2021-04-01"
