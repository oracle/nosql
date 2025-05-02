compiled-query-plan

{
"query file" : "idc_geojson/q/q143.q",
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
          "equality conditions" : {"id":6},
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
            "value" : {"coordinates":[[[-114.04233455657959,51.04823368482349],[-114.04260277748106,51.04764687882683],[-114.04252767562866,51.04619669418911],[-114.04002785682677,51.04588641623084],[-114.03739929199219,51.04634508726053],[-114.03717398643494,51.047087045486776],[-114.03679847717285,51.04772107309905],[-114.03832197189331,51.04801110411368],[-114.04023170471191,51.04815274650733],[-114.04233455657959,51.04823368482349]]],"type":"Polygon"}
          }
        }
      }
    ]
  }
}
}