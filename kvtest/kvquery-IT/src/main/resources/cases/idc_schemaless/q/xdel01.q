delete from jsoncol jc where regex_like(jc.menu.menu.value,".*e3.*") returning *
