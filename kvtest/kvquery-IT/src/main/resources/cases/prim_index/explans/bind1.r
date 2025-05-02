compiled-query-plan

{
"query file" : "prim_index/q/bind1.q",
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
          "equality conditions" : {"id1":0},
          "range conditions" : { "id2" : { "end value" : 42.0, "end inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$ext1_1"
        }
      ],
      "map of key bind expressions" : [
        [ 0, -1, -1 ]
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "GREATER_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$Foo"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          },
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$Foo"
              }
            },
            "right operand" :
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$ext1_2"
            }
          },
          {
            "iterator kind" : "GREATER_THAN",
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
              "value" : "id4"
            }
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$Foo",
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
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