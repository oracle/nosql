declare $arr5 array(json);
select id
from ComplexType f
where exists f.address.phones[($element.work ,$element.home) in $arr5[]]
