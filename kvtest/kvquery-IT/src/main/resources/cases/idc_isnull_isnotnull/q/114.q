#Expression is map filter step with .values() and and $key implicit variable with is not null in projection
select id,s.children.values($key = "George") is not null from sn s