#
# Chania county
#
select id,
       c.info.locations.features[
         geo_intersect($element.geometry,
                       { "type" : "polygon",
                         "coordinates" : [ [
                                             [23.48, 35.16],
                                             [24.30, 35.16],
                                             [24.30, 35.70],
                                              [23.48, 35.70],
                                            [23.48, 35.16]
                                         ] ]
                       } )
         and
         $element.properties.kind = "sales" 
       ].geometry as loc
from companies2 c
where exists c.info.locations.features[
        geo_intersect($element.geometry,
                      { "type" : "polygon",
                        "coordinates" : [ [
                                            [23.48, 35.16],
                                            [24.30, 35.16],
                                            [24.30, 35.70],
                                            [23.48, 35.70],
                                            [23.48, 35.16]
                                        ] ]
                      } )
        and
        $element.properties.kind = "sales" 
      ]
