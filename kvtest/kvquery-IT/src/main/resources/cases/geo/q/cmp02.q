#
# campbell
#
select /* FORCE_PRIMARY_INDEX(companies) */
       id
from companies c
where exists c.locations.loc[
      geo_inside($element, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [-121.991191, 37.272340],
                                          [-121.957718, 37.262778],
                                          [-121.934543, 37.286955],
                                          [-121.944500, 37.302114],
                                          [-121.970249, 37.294057],
                                          [-121.991191, 37.272340]
                                      ] ]
                    } )
      ]
