declare $arr3 array(json); 
        $arr4 array(json);
select id
from ComplexType f
where exists f.address.phones[($element.work ,$element.home) in $arr3[]] and
      exists f.address.phones[$element.work in $arr4[]] and 
      9 <= f.age and f.age <= 15
