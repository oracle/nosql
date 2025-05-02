#Expression is map filter step with .values() with is null in projection
select id, s.children.values().friends[].George is null from sn s