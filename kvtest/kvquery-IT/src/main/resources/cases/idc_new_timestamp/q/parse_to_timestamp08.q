select parse_to_timestamp(t.doc.str4, 'MM/dd yyyy')
from roundFunc t
where id = 6
