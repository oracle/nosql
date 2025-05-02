compiled-query-plan

{
"query file" : "idc_geojson/q/q147.q",
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
          "equality conditions" : {"id":7},
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
            "value" : {"coordinates":[[[-114.04192686080933,51.048550691866645],[-114.04185175895691,51.0475726844358],[-114.04199123382568,51.0458594354406],[-114.03903007507324,51.045596371912126],[-114.03739929199219,51.046176458733434],[-114.03709888458252,51.04694539983421],[-114.0370774269104,51.04799086945067],[-114.03911590576172,51.04850347818921],[-114.04192686080933,51.048550691866645]]],"type":"Polygon"}
          }
        }
      }
    ]
  }
}
}