#test to check on degree function index on array elements

select m.id, m.doubArr
from math_test m
where exists m.doubArr[degrees($element) = degrees(2.7182823456)]
order by id
