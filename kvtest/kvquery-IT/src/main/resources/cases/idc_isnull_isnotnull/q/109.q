#Expression is map filter step on a map with .values() and is null in projection
select s.children.Matt.age.values() is null from sn s