select majorKey1, $phone.value from jsoncol as $u, unnest($u.menu.menu.popup.menuitem as $menuitem)
