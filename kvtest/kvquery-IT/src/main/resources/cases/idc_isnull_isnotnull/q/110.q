#Expression is map filter step on a map with .values() and is not null in projection
select s.children.Matt.age.values() is not null  from sn s