#Expression with is not null and .values(), mixing differnt fields
select id, s.children.values().Relative is null from sn s where age is not null and s.children.Mary.age is not null