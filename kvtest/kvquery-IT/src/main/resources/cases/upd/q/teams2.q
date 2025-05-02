declare $add integer; $position integer;
update teams t
add t.info.teams[1].userids $position $add
where id = 2
returning *
