update Foo $f
set $f.info.address.phones[] =
    case
    when $ is of type (map(any)) and $.areacode = 415 and $.number >= 5000000 then
      seq_concat($, { "areacode" : 416 })
    when $ is of type (string) then
      "416-500-0000"
    end
where id = 11
returning $f.info.address, remaining_days($f)

