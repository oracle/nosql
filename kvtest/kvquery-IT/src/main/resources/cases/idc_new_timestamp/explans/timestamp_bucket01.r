compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_bucket01.q",
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
      "target table" : "roundFunc",
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
        "field name" : "jl3",
        "field expression" : 
        {
          "iterator kind" : "CAST",
          "target type" : "Timestamp(3)",
          "quantifier" : "",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "l3",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "doc",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          }
        }
      },
      {
        "field name" : "jl3_to_2_weeks",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "l3",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "doc",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "2 weeks"
            }
          ]
        }
      },
      {
        "field name" : "jl3_to_5_day",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "l3",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "doc",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "5 days"
            }
          ]
        }
      },
      {
        "field name" : "jl3_to_5_hour",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "l3",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "doc",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "5 hours"
            }
          ]
        }
      },
      {
        "field name" : "s6",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "s6",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "doc",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "s6_to_day1",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "s6",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "doc",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          }
        }
      },
      {
        "field name" : "s6_to_10_minute",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s6",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "10 minutes"
            }
          ]
        }
      },
      {
        "field name" : "s6_to_30_second",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_BUCKET",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s6",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "30 seconds"
            }
          ]
        }
      }
    ]
  }
}
}