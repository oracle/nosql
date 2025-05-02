# Test Description: Test to verify; after L and H are computed, the step selects all the elements between positions L and H (L and H included).

select id, $C.arrdbl[0:3]
from Complex $C
order by id
