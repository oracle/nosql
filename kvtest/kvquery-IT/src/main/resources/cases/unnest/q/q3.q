
select id, age, $phone.work
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "MA" and
      $t.age < 13
      
