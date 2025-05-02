compiled-query-plan

{
"query file" : "idc_groupby_orderby_distinct/q/q19.q",
"plan" : 
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
        "input variable" : "$gb-1",
        "input iterator" :
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "TABLE",
            "target table" : "ComplexType",
            "row variable" : "$$t",
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
          "FROM variable" : "$$t",
          "SELECT expressions" : [
            {
              "field name" : "age",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "age",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            },
            {
              "field name" : "city",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "city",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "address",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            },
            {
              "field name" : "Column_3",
              "field expression" : 
              {
                "iterator kind" : "ADD_SUBTRACT",
                "operations and operands" : [
                  {
                    "operation" : "+",
                    "operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "age",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$t"
                      }
                    }
                  },
                  {
                    "operation" : "+",
                    "operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "lng",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$t"
                      }
                    }
                  }
                ]
              }
            }
          ]
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-1"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "city",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-1"
            }
          }
        ],
        "aggregate functions" : [
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "Column_3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-1"
              }
            }
          }
        ]
      }
    },
    "grouping expressions" : [
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "age",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-3"
        }
      },
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "city",
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
          "field name" : "Column_3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-3"
          }
        }
      }
    ]
  },
  "FROM variable" : "$from-2",
  "SELECT expressions" : [
    {
      "field name" : "$from-2",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$from-2"
      }
    }
  ],
  "OFFSET" :
  {
    "iterator kind" : "CONST",
    "value" : 1
  },
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 2
  }
}
}