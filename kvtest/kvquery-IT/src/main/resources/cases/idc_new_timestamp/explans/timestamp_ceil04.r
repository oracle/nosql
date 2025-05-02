compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_ceil04.q",
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
        "field name" : "t0",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "t0",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "t0_to_year",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t0",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "year"
            }
          ]
        }
      },
      {
        "field name" : "t0_to_iyear",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t0",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "iyear"
            }
          ]
        }
      },
      {
        "field name" : "t3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "t3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "t0_to_quarter",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "QUARTER"
            }
          ]
        }
      },
      {
        "field name" : "t3_to_month",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "Month"
            }
          ]
        }
      },
      {
        "field name" : "l3",
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
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "l3_to_week",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "l3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "weeK"
            }
          ]
        }
      },
      {
        "field name" : "l3_to_iweek",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "l3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "iweeK"
            }
          ]
        }
      },
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
        "field name" : "jl3_to_day",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
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
              "value" : "day"
            }
          ]
        }
      },
      {
        "field name" : "jl3_to_hour",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
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
              "value" : "hour"
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
          "iterator kind" : "FN_TIMESTAMP_CEIL",
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
        "field name" : "s6_to_minute",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
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
              "value" : "minute"
            }
          ]
        }
      },
      {
        "field name" : "s6_to_second",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_CEIL",
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
              "value" : "second"
            }
          ]
        }
      }
    ]
  }
}
}