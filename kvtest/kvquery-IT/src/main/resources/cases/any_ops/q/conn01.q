select *
from connections c
where c.connections[] =any [3,5][]
