select id
from roundFunc t
where exists t.doc.map.keys(day_of_month($key) =any 26)