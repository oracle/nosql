select id
from User as $u, unnest(children.values() as $child)
