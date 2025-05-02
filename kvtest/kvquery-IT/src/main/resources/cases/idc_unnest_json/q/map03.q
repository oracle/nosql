select id, $child.age
from User_json as $u, unnest($u.info.children.values() as $child)
where $child.school > "sch_1" and $child.school < "sch_3"
