compiled-query-plan

{
"query file" : "idc_groupby_orderby_distinct/q/q3.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "GROUP",
      "input variable" : "$gb-3",
      "input iterator" :
      {
        "iterator kind" : "RECEIVE",
        "distribution kind" : "ALL_PARTITIONS",
        "input iterator" :
        {
          "iterator kind" : "GROUP",
          "input variable" : "$gb-2",
          "input iterator" :
          {
            "iterator kind" : "SELECT",
            "FROM" :
            {
              "iterator kind" : "TABLE",
              "target table" : "SimpleDatatype",
              "row variable" : "$$f",
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
            "FROM variable" : "$$f",
            "SELECT expressions" : [
              {
                "field name" : "gb-0",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "isEmp",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              },
              {
                "field name" : "gb-1",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "lastname",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              },
              {
                "field name" : "gb-2",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "bin",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              },
              {
                "field name" : "aggr-3",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "lastname",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              }
            ]
          },
          "grouping expressions" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "gb-0",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-2"
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "gb-1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-2"
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "gb-2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-2"
              }
            }
          ],
          "aggregate functions" : [
            {
              "iterator kind" : "FN_COUNT",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "aggr-3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$gb-2"
                }
              }
            }
          ]
        }
      },
      "grouping expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-0",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-3"
          }
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-3"
          }
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-3"
          }
        }
      ],
      "aggregate functions" : [
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-3"
            }
          }
        }
      ]
    },
    "FROM variable" : "$from-0",
    "SELECT expressions" : [
      {
        "field name" : "lastname",
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
      },
      {
        "field name" : "isEmp",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-0",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-0"
          }
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-0"
          }
        }
      }
    ]
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "lastname",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "isEmp",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "Column_3",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    }
  ],
  "aggregate functions" : [

  ]
}
}