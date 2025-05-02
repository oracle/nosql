select id, $child_info.friends
from User as $u, unnest($u.children.values() as $child_info)
where $u.children.Anna.age = 11
