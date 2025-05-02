#Expression is map filter step with .values() and $key implicit variable with is null in projection
select id,s.children.values($key = "age") is null from sn s
