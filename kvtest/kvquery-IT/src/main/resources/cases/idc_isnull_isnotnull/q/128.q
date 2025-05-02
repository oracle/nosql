#Expression is map filter step on a nested map with .values().values() and $value implicit variable with is null in projection
select s.children.Anna.values().values($value) is null from sn s