#Expression has is of type operator with .values() and $key implicit variable and is null in predicate
select id from sn s where  s.children.Mary.values($key ="age") is of type (integer) is null