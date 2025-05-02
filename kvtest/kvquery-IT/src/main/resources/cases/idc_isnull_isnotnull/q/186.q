#Expression with is not null and .values(), mixing differnt fields
select id, s.children.values().Relative from sn s where age is not null and s.address.state is not null