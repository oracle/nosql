select id
from User as $u, unnest($u.children.values() as $child)
