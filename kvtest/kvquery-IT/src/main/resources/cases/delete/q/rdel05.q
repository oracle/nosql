declare $areacode integer; // = 650
delete from foo f
where f.info.address.phones.areacode =any $areacode
returning id, f.info.address.phones
