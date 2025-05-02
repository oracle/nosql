select seq_transform(t.info.formats[], format_timestamp($.dt, $.pattern)) as d
from arithtest t
where id = 0
