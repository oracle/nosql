compiled-query-plan

{
"query file" : "idc_loj/q/q17.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "A",
      "row variable" : "$$a",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "descendant tables" : [
        { "table" : "A.B.D", "row variable" : "$$d", "covering primary index" : false }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$a", "$$d"],
    "WHERE" : 
    {
      "iterator kind" : "LESS_THAN",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "d3",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$d"
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 1000
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "lastTwoStrings",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : false,
          "input iterators" : [
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound iterator" : 
              {
                "iterator kind" : "ADD_SUBTRACT",
                "operations and operands" : [
                  {
                    "operation" : "+",
                    "operand" :
                    {
                      "iterator kind" : "FUNC_SIZE",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$"
                      }
                    }
                  },
                  {
                    "operation" : "-",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 2
                    }
                  }
                ]
              },
              "high bound" : 2147483647,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "d4",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$d"
                }
              }
            }
          ]
        }
      },
      {
        "field name" : "a_ida1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        }
      }
    ]
  }
}
}