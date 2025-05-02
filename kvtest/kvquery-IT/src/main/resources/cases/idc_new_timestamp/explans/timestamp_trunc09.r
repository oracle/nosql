compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_trunc09.q",
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
      "target table" : "jsonCollection_test",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
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
        "field name" : "l3_to_day",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_TRUNC",
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
              "value" : "day"
            }
          ]
        }
      },
      {
        "field name" : "l3_to_hour",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_TRUNC",
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
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "s6_to_day1",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_TRUNC",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "s6",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "s6_to_minute",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s6",
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
        "field name" : "s6_to_second",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s6",
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