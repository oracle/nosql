compiled-query-plan

{
"query file" : "idc_maths/q/aggregate01.q",
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
              "iterator kind" : "FUNC_SUM",
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
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "FLOOR",
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "value1",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$aggregate_test"
                    }
                  }
                ]
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
                "field name" : "value3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$aggregate_test"
                }
              }
            }
          },
          {
            "field name" : "aggr-3",
            "field expression" : 
            {
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "CEIL",
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "value3",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$aggregate_test"
                    }
                  }
                ]
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
      },
      {
        "field name" : "aggr-3",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-3",
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
      "field name" : "floorsum",
      "field expression" : 
      {
        "iterator kind" : "FLOOR",
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
      }
    },
    {
      "field name" : "sumfloor",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "aggr-1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "ceilsum",
      "field expression" : 
      {
        "iterator kind" : "CEIL",
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
      }
    },
    {
      "field name" : "sumceil",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "aggr-3",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}