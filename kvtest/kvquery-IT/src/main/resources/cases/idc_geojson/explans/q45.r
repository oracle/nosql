compiled-query-plan

{
"query file" : "idc_geojson/q/q45.q",
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
      "target table" : "points",
      "row variable" : "$$p",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":18},
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
          "iterator kind" : "FN_GEO_WITHIN_DISTANCE",
          "search target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "point",
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
            "value" : {"coordinates":[[77.57508516311646,12.972943850854447],[77.57585763931274,12.971877439057483],[77.57607221603394,12.970957393833265],[77.57607221603394,12.970246447465623],[77.57686614990234,12.969012741593874],[77.57871150970459,12.969305485913676],[77.58057832717896,12.970058255439973],[77.5820803642273,12.969075472548546]],"type":"LineString"}
          },
          "distance iterator" :
          {
            "iterator kind" : "CONST",
            "value" : 300000.098
          }
        }
      }
    ]
  }
}
}