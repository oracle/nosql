#Expression is map filter step on a nested map with .values().values() and is null in projection
select s.children.Anna.values().values() is null from sn s