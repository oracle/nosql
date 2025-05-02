#Expression returns is null and is not null with string in record
select id, s.children.values().Relative from sn s where age is null and s.address.state is not null
