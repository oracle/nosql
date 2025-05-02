# Negative case: can't remove the fields from RECORD.

update Bar b
remove b.record.int
where id = 20
