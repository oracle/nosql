select id, 
       format_timestamp(tm9, 'MM/dd/yyyy HH:mm VV', 'America/Los_Angeles') as d0,
       format_timestamp(tm9, 'MMM dd yyyy VV', 'GMT+05:00') as d1, 
       format_timestamp(t.info.tm3, "MM/dd/yy'T'HH:mm:SS.SSS VV", 'UTC') as d2,
       format_timestamp(t.info.tm3, 'HH-mm-ss.SSSSSS VV', 'Asia/Kolkata') as d3,
       format_timestamp(t.info.tm3, 'yyyy-MM-dd', 'Asia/Kolkata') as d4,
       format_timestamp(t.info.tm3, 'MMM,dd HH-mm-SS') as d5
from arithtest t
where id = 0
