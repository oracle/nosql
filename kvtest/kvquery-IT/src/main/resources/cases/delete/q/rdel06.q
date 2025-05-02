delete from foo f
where f.info.address.phones.areacode <any 600
returning id
