select *
from connections c
where c.connections[] =any [9, 8, 1][]
