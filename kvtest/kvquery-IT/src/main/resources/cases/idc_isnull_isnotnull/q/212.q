#Expression has is not of type operator with .values() and is not null in predicate
select id from sn s where  s.children.Mary.values() is not of type (boolean) is not null