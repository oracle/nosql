select majorKey1 from jsoncol as $u, unnest($u.menu.menu.popoup.menuitem[] as $menuitem),unnest($u.phones[] as $phone)
