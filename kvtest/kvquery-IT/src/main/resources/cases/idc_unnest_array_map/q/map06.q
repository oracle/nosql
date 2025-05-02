select id, $child_info.friends
from User as $u, $u.children.values() as $child_info
where $child_info.age = 11
