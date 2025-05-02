compiled-query-plan

{
"query file" : "idc_geojson/q/SR27209.q",
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
      "target table" : "geosrs",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"pk":2},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "GEO_DISTANCE",
          "first geometry iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "geometry",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 0,
              "high bound" : 0,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "features",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "geojson",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          },
          "second geometry iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "geometry",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 1,
              "high bound" : 1,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "features",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "geojson",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          }
        }
      }
    ]
  }
}
}