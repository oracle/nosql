select f.firstName,f.uid2
from bar1 f
group by f.firstName,f.uid2
offset 2
