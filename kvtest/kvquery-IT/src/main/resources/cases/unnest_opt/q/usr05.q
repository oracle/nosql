# TODO: make this query use the index
select id
from User as $u, unnest($u.children.anna.friends[] $friend)
where $friend = "mark"
