compiled-query-plan

{
"query file" : "idc_maths/q/aggregate04.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_PARTITIONS",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "aggregate_test",
          "row variable" : "$$aggregate_test",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$aggregate_test",
        "GROUP BY" : "No grouping expressions",
        "SELECT expressions" : [
          {
            "field name" : "aggr-0",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "value1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$aggregate_test"
                }
              }
            }
          },
          {
            "field name" : "aggr-1",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "value2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$aggregate_test"
                }
              }
            }
          },
          {
            "field name" : "aggr-2",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "value3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$aggregate_test"
                }
              }
            }
          }
        ]
      }
    },
    "FROM variable" : "$from-1",
    "GROUP BY" : "No grouping expressions",
    "SELECT expressions" : [
      {
        "field name" : "aggr-0",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-1",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      }
    ]
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "sincount",
      "field expression" : 
      {
        "iterator kind" : "TRUNC",
        "input iterators" : [
          {
            "iterator kind" : "SIN",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "aggr-0",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$from-0"
                }
              }
            ]
          },
          {
            "iterator kind" : "CONST",
            "value" : 7
          }
        ]
      }
    },
    {
      "field name" : "coscount",
      "field expression" : 
      {
        "iterator kind" : "TRUNC",
        "input iterators" : [
          {
            "iterator kind" : "COS",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "aggr-1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$from-0"
                }
              }
            ]
          },
          {
            "iterator kind" : "CONST",
            "value" : 7
          }
        ]
      }
    },
    {
      "field name" : "atancount",
      "field expression" : 
      {
        "iterator kind" : "TRUNC",
        "input iterators" : [
          {
            "iterator kind" : "ATAN",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "aggr-2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$from-0"
                }
              }
            ]
          },
          {
            "iterator kind" : "CONST",
            "value" : 7
          }
        ]
      }
    }
  ]
}
}