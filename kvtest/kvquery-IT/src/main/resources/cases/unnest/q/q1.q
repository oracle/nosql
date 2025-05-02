select id, age, $phone.work
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      $t.address.city > "F" and
      $t.address.city > "G"
