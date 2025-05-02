compiled-query-plan

{
"query file" : "offsetlimit/q/idx_offlim1.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 4, 0, 1, 2 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$Foo",
        "index used" : "idx2",
        "covering index" : true,
        "index row variable" : "$$Foo_idx",
        "index scans" : [
          {
            "equality conditions" : {"age":15},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$Foo_idx",
      "SELECT expressions" : [
        {
          "field name" : "id1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo_idx"
            }
          }
        },
        {
          "field name" : "id2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo_idx"
            }
          }
        },
        {
          "field name" : "id3",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo_idx"
            }
          }
        },
        {
          "field name" : "id4",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id4",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo_idx"
            }
          }
        },
        {
          "field name" : "age",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo_idx"
            }
          }
        }
      ],
      "LIMIT" :
      {
        "iterator kind" : "ADD_SUBTRACT",
        "operations and operands" : [
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "CONST",
              "value" : 10
            }
          },
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "$from-0",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$from-0"
      }
    }
  ],
  "OFFSET" :
  {
    "iterator kind" : "CONST",
    "value" : 10
  },
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 5
  }
}
}