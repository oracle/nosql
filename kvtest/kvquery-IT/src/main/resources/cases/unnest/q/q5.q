select *
from foo as $t, $t.address.phones[] as $phone, $t.address as $addr
where $addr.state = "MA" and
      $addr.city > "F"
