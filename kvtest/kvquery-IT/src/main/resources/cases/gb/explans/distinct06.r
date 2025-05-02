compiled-query-plan

{
"query file" : "gb/q/distinct06.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-0",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "SORT",
      "order by fields at positions" : [ 2, 1 ],
      "input iterator" :
      {
        "iterator kind" : "RECEIVE",
        "distribution kind" : "SINGLE_PARTITION",
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
                "equality conditions" : {"id1":0},
                "range conditions" : {}
              }
            ],
            "position in join" : 0
          },
          "FROM variable" : "$$f",
          "SELECT expressions" : [
            {
              "field name" : "long",
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
            },
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
              "field name" : "sort_gen",
              "field expression" : 
              {
                "iterator kind" : "ADD_SUBTRACT",
                "operations and operands" : [
                  {
                    "operation" : "+",
                    "operand" :
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
                    "operation" : "+",
                    "operand" :
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
              }
            }
          ]
        }
      }
    },
    "FROM variable" : "$from-1",
    "SELECT expressions" : [
      {
        "field name" : "long",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "long",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "int",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "int",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    ]
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "long",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-0"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "int",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-0"
      }
    }
  ],
  "aggregate functions" : [

  ]
}
}