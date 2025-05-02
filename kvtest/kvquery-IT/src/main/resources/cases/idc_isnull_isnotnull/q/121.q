#Expression is map filter step on a nested map with .values().values() and is null in predicate
select s.children.values().values() from sn s where s.children.Anna.values().values() is null