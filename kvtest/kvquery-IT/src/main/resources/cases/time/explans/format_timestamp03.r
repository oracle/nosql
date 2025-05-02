compiled-query-plan

{
"query file" : "time/q/format_timestamp03.q",
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
      "target table" : "arithtest",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":0},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "d",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "SEQ_MAP",
              "mapper iterator" :
              {
                "iterator kind" : "FN_FORMAT_TIMESTAMP",
                "input iterators" : [
                  {
                    "iterator kind" : "PROMOTE",
                    "target type" : "Any",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "dt",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$sq1"
                      }
                    }
                  },
                  {
                    "iterator kind" : "PROMOTE",
                    "target type" : "Any",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "pattern",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$sq1"
                      }
                    }
                  }
                ]
              },
              "input iterator" :
              {
                "iterator kind" : "ARRAY_FILTER",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "formats",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "info",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$t"
                    }
                  }
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}