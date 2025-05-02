declare $arr2 array(json);
        $k3 string;
        $k4 integer;
        $k5 long;
select id
from ComplexType f
where f.address.state = $k3 and
      (f.id, cast(f.flt as double), f.firstName) in $arr2[] and
      $k4 <= f.age and f.lng <= $k5
