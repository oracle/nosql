#Expression is map filter step on a nested map with .values().values() and is not null in projection
select s.children.Anna.values().values() is not null from sn s