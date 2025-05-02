declare $state1 anyJsonAtomic; // 3
        $state2 anyJsonAtomic; // "WA"
select id
from bar b
where $state1 < b.info.address.state and b.info.address.state < $state2
