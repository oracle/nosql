declare $add integer;
update teams t
add t.info.teams[1].userids $add
where id = 1
returning *
