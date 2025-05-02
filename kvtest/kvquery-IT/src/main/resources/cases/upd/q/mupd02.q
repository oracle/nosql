update users t
add t.info.friends 'Jerry',
put t.info {"hobbies":["Cooking", "Music"]},
remove t.info.address.street
where sid = $mupd2_sid and id < $mupd2_id
