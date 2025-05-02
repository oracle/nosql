select id
from ComplexType f
where (f.id in (1, 0)) and (f.id in (3.5, 10.5, 0,1)) and exists f.flt

