select id
from User as $u, unnest($u.children.keys() as $child)
