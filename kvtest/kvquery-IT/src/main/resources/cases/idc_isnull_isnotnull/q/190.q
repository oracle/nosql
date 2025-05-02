#Expression with is not null and .values(), mixing differnt fields
select id, s.children.values().Relative is not null from sn s where age  is not null