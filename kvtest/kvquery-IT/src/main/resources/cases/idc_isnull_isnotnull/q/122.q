#Expression is map filter step on a nested map with .values().values() and is not null in predicate
select s.children.values().values() from sn s where s.children.Anna.values().values() is not null