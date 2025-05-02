#Test Description: Pass map as argument to size() function.

select id1, size(f.rec.fmap)
from foo f
order by id1