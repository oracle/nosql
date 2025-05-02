select id
from User as $u, unnest($u.children.values($value.values()) as $child)
