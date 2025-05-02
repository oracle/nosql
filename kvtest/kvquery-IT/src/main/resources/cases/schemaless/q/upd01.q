update viewers v
set v.firstName = "Jonathan",
add v.shows 
    {
      "showName": "Casa de papel",
      "showId": 18,
      "type": "tvseries",
      "genres" : ["action", "spanish"]
     },
set v.shows[0].seasons[0].numEpisodes = 3,
add v.shows[0].seasons[0].episodes
    {
      "episodeID": 40,
      "lengthMin": 52,
      "minWatched": 45,
      "date": "2021-05-23"
    },
remove v.shows[1].seasons[1]
where acct_id = 200 and user_id = 1


select v.firstName, v.shows
from viewers v
where acct_id = 200 and user_id = 1
