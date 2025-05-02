select *
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "MA" and
      $t.address.city > "F"
