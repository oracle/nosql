# Test for regex_like function with delete clause

delete from playerinfo.desc p where regex_like(p.desc,".*del.*")
returning *
