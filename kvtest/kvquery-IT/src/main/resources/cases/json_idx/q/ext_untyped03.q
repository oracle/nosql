declare $state1 anyJsonAtomic; // = 3
select id
from bar b
where b.info.address.state > $state1
