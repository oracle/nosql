#Expression is map filter step with .values() and and $key implicit variable with is null in projection
select id,s.children.values($key = "George") is null from sn s