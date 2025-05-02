select parse_to_timestamp(t.info.dt.str, t.info.dt.pattern) as d1,
       parse_to_timestamp(t.info.tm6) as d2,
       parse_to_timestamp(t.info.tm6, t.info.notexists) as d3,
       parse_to_timestamp('Apr 16, 24', "MMM dd, yy") as d4
from arithtest t
where id = 0
