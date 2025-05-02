#Expression is map filter step with .values() with is null in predicate
select id, s.children.values().friends[] from sn s where s.children.values().friends.Bobby is null