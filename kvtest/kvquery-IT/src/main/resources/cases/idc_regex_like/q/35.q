#TestDescription: source is of type fixed binary
#Expected result: return error

select regex_like(p.fbin,"SGVsbG8gSG93IGFyZSBZb3U/") from playerinfo p