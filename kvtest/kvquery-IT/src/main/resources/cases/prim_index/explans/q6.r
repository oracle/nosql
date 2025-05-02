compiled-query-plan

{
"query file" : "prim_index/q/q6.q",
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
      "target table" : "Foo",
      "row variable" : "$$Foo",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id1":3,"id2":30.0},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id4",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "id4-3"
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$Foo",
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
        {
          "iterator kind" : "ADD_SUBTRACT",
          "operations and operands" : [
            {
              "operation" : "+",
              "operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "id1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$Foo"
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
      },
      {
        "field name" : "id2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo"
          }
        }
      },
      {
        "field name" : "id4",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id4",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo"
          }
        }
      }
    ]
  }
}
}