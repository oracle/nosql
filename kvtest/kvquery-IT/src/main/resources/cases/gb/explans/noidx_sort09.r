compiled-query-plan

{
"query file" : "gb/q/noidx_sort09.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 1, 0 ],
    "input iterator" :
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
              "target table" : "Foo",
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
                "field name" : "int",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "int",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "record",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f"
                    }
                  }
                }
              },
              {
                "field name" : "Column_2",
                "field expression" : 
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "long",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "record",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f"
                    }
                  }
                }
              }
            ]
          },
          "grouping expressions" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "int",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-2"
              }
            }
          ],
          "aggregate functions" : [
            {
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "Column_2",
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
          "field name" : "int",
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
            "field name" : "Column_2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-3"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "int",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "int",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "Column_2",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "Column_2",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ],
  "OFFSET" :
  {
    "iterator kind" : "CONST",
    "value" : 2
  }
}
}