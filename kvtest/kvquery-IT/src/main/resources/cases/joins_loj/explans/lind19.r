compiled-query-plan

{
"query file" : "joins_loj/q/lind19.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "A.B",
      "row variable" : "$$b",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "descendant tables" : [
        { "table" : "A.B.C", "row variable" : "$$c", "covering primary index" : true },
        { "table" : "A.B.C.D", "row variable" : "$$d", "covering primary index" : true }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$b", "$$c", "$$d"],
    "WHERE" : 
    {
      "iterator kind" : "CASE",
      "clauses" : [
        {
          "when iterator" :
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$d"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 40
            }
          },
          "then iterator" :
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "ADD_SUBTRACT",
              "operations and operands" : [
                {
                  "operation" : "+",
                  "operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "ida",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$b"
                    }
                  }
                },
                {
                  "operation" : "+",
                  "operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "idc",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$c"
                    }
                  }
                }
              ]
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 25
            }
          }
        },
        {
          "else iterator" :
          {
            "iterator kind" : "GREATER_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idc",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$c"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          }
        }
      ]
    },
    "SELECT expressions" : [
      {
        "field name" : "b_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b"
          }
        }
      },
      {
        "field name" : "b_idb",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idb",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b"
          }
        }
      },
      {
        "field name" : "c_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$c"
          }
        }
      },
      {
        "field name" : "c_idb",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idb",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$c"
          }
        }
      },
      {
        "field name" : "c_idc",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$c"
          }
        }
      },
      {
        "field name" : "d_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$d"
          }
        }
      },
      {
        "field name" : "d_idb",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idb",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$d"
          }
        }
      },
      {
        "field name" : "d_idc",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$d"
          }
        }
      },
      {
        "field name" : "d_idd",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idd",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$d"
          }
        }
      }
    ]
  }
}
}