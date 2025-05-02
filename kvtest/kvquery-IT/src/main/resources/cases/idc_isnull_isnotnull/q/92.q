#Expression is map filter step with .values() with is not null in projection
select id, s.children.values().friends[].George is not null from sn s