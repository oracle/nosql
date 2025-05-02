declare $arr1 array(INTEGER);
select id
from ComplexType f
where exists f.address.phones[$element.work in $arr1[]]
