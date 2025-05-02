
select count(*)
from netflix f
where f.value.contentStreamed.showId =any 15 and
      size(f.value.contentStreamed[$element.showId = 15].seriesInfo) = f.value.contentStreamed[$element.showId = 15].numSeasons and
      not seq_transform(f.value.contentStreamed[$element.showId = 15].seriesInfo[], $.numEpisodes = size($.episodes)) =any false and
      seq_sum(seq_transform(f.value.contentStreamed[$element.showId = 15].seriesInfo.episodes[], $.lengthMin - $.minWatched)) = 0
