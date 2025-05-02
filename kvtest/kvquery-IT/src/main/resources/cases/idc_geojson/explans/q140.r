compiled-query-plan

{
"query file" : "idc_geojson/q/q140.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "geotypes",
      "row variable" : "$$p",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":5},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "FN_GEO_INSIDE",
          "search target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "geom",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          },
          "search geometry iterator" :
          {
            "iterator kind" : "CONST",
            "value" : {"coordinates":[[[77.59536266326904,12.995734695504481],[77.59433269500732,12.991762129920927],[77.59931087493896,12.989964000395487],[77.60561943054199,12.991929397121895],[77.60145664215088,12.995734695504481],[77.59536266326904,12.995734695504481]]],"type":"Polygon"}
          }
        }
      }
    ]
  }
}
}