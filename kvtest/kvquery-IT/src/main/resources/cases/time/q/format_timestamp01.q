select id, 
       format_timestamp(tm9) as d0,
       format_timestamp(tm9, 'MMMdd yyyyVV') as d1, 
       format_timestamp(t.info.tm3) as d2,
       format_timestamp(t.info.tm3, 'HH-mm-ss.SSSSSS') as d3, 
       format_timestamp(t.info.tm6) as d4,
       format_timestamp(t.info.tm6, "yyyy-ww'w'") as d5,
       format_timestamp(t.info.tm6, t.info.notexists) as d6,       
       format_timestamp(t.info.notexists, 'yyyy-MM-dd') as d7
from arithtest t
where id = 0