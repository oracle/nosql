compiled-query-plan

{
"query file" : "idc_in_expr/q/q29.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "fooNew",
      "row variable" : "$$f",
      "index used" : "idx_barNew1234",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "IN",
        "left-hand-side expressions" : [
          {
            "iterator kind" : "ADD_SUBTRACT",
            "operations and operands" : [
              {
                "operation" : "+",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info.bar1",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                }
              },
              {
                "operation" : "+",
                "operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              }
            ]
          }
        ],
        "right-hand-side expressions" : [
          {
            "iterator kind" : "CONST",
            "value" : 6
          },
          {
            "iterator kind" : "CONST",
            "value" : 5
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      }
    ]
  }
}
}