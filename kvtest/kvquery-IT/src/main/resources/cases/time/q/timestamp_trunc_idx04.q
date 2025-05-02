select id 
from roundtest t 
where timestamp_trunc(t.doc.l3,'hour') = "2020-02-28T23" and 
      timestamp_trunc(l3) > "2000-01-01"
