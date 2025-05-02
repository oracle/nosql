compiled-query-plan

{
"query file" : "idc_geojson/q/q03.q",
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
          "equality conditions" : {"id":6},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$p"
          }
        }
      },
      {
        "field name" : "point",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
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
            }
          ]
        }
      },
      {
        "field name" : "dist",
        "field expression" : 
        {
          "iterator kind" : "CAST",
          "target type" : "Long",
          "quantifier" : "",
          "input iterator" :
          {
            "iterator kind" : "MULTIPLY_DIVIDE",
            "operations and operands" : [
              {
                "operation" : "*",
                "operand" :
                {
                  "iterator kind" : "GEO_DISTANCE",
                  "first geometry iterator" :
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
                  "second geometry iterator" :
                  {
                    "iterator kind" : "CONST",
                    "value" : {"coordinates":[-4.77604866027832,37.884913694932365],"type":"point"}
                  }
                }
              },
              {
                "operation" : "*",
                "operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 1000000
                }
              }
            ]
          }
        }
      }
    ]
  }
}
}