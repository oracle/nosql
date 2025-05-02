select id, $child_info.friends
from User_json as $u, unnest($u.info.children.values() as $child_info)
where $child_info.age > 10 and $u.info.children.Anna.school > "sch_1"
