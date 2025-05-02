select id, $child_info.friends
from User as $u, unnest($u.children.values() as $child_info)
where $child_info.age = 11 and $u.children.Anna.school > "sch_1"
