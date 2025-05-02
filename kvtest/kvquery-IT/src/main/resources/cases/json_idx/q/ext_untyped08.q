declare $state1 anyJsonAtomic; // 3
        $state3 anyJsonAtomic; // 5.5
select id
from bar b
where $state1 < b.info.address.state and b.info.address.state < $state3
