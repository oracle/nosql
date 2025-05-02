select 
  {
    "user_id" : user_id, 
    "value" :
        { "contentStreamed" : seq_transform(n.value.contentStreamed.seriesInfo,
                              { "seriesInfo" : seq_transform($[],
                                               { "seasonNum" : $.seasonNum,
                                                 "numEpisodes" : $.numEpisodes } )
                              } )
        }
  }  
from netflix n
where user_id = 1


#db.netflix.find({ user_id : 1}, {user_id:1, "value.contentStreamed.seriesInfo.numEpisodes" : 1, "value.contentStreamed.seriesInfo.seasonNum"})

# ???? Can mongo do array filtering in projection? Seems so: $elemMatch, $, $slice

# mongo has HAVING clause
