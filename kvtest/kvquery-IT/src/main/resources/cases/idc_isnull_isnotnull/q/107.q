#Expression is map filter step on a map with .values() and is null in predicate
select s.children.values() from sn s where s.children.George.age.values() is  null