select id, $child.friends
from User_json as $u, unnest($u.info.children.values() as $child)
where $child.age = 11 and $u.info.children.Anna.school > "sch_1"
