#Expression is map filter step with .values() and $key implicit variable with is not null in projection
select id,s.children.values($key = "age") is not null from sn s
