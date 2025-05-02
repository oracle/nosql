compiled-query-plan

{
"query file" : "time/q/timestamp_floor02.q",
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
      "target table" : "roundtest",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":2},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "t3_to_year",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
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
              "value" : "year"
            }
          ]
        }
      },
      {
        "field name" : "t3_to_iyear",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
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
              "value" : "iyear"
            }
          ]
        }
      },
      {
        "field name" : "t3_to_quarter",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
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
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
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
        "field name" : "t3_to_week",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
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
              "value" : "weeK"
            }
          ]
        }
      },
      {
        "field name" : "t3_to_iweek",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
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
              "value" : "iweeK"
            }
          ]
        }
      },
      {
        "field name" : "t9_to_day",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s9",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
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
        "field name" : "t9_to_day1",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "s9",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "t9_to_hour",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s9",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
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
        "field name" : "t9_to_minute",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s9",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
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
        "field name" : "t3_to_second",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_FLOOR",
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
              "value" : "second"
            }
          ]
        }
      }
    ]
  }
}
}