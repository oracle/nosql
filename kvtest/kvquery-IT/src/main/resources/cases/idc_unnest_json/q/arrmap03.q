select id, $phone.areacode
from User_json as $u, unnest($u.info.addresses.phones[][] as $phone),
                             $u.info.children.values() as $child
where $phone.kind < 'w' and
      $child.school > "sch_1" and $child.school < "sch_3"
