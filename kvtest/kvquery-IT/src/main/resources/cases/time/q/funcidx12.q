select id
from bar2 b
where substring(b.map.a, 0, 4) = "2020"
