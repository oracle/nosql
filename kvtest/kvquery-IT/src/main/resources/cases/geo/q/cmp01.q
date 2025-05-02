#
# palo alto
#
select /* FORCE_PRIMARY_INDEX(companies) */
       id,
       c.locations[geo_inside($element.loc,
                                 { "type" : "polygon",
                                   "coordinates" : [ [
                                          [-122.194164, 37.431279],
                                          [-122.131680, 37.396102],
                                          [-122.104214, 37.428825],
                                          [-122.146443, 37.456492],
                                          [-122.194164, 37.431279]
                                   ] ] } )
                  ]
from companies c
where exists c.locations.loc[
      geo_inside($element, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [-122.194164, 37.431279],
                                          [-122.131680, 37.396102],
                                          [-122.104214, 37.428825],
                                          [-122.146443, 37.456492],
                                          [-122.194164, 37.431279]
                                      ] ]
                    } )
      ]
