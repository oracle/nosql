select id
from User_json as $u, unnest($u.info.children.values() as $child)
