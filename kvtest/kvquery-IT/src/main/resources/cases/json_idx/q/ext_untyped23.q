declare $state1 anyJsonAtomic; // 3
        $state2 anyJsonAtomic; // "WA"
select id
from bar b
where b.info.address.state > $state1 and b.info.address.state = $state2
