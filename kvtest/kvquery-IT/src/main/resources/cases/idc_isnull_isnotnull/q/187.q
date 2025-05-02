#Expression returns is null with .values(), mixing differnt fields
select id, s.children.values().Relative from sn s where age is not null and s.children.values().age is null