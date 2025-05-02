compiled-query-plan

{
"query file" : "single_shard/q/sort2.q",
"plan" : 
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
      "row variable" : "$$Foo",
      "index used" : "idx2",
      "covering index" : true,
      "index row variable" : "$$Foo_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "age" : { "start value" : 0, "start inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$age"
        }
      ],
      "map of key bind expressions" : [
        [ 0, -1 ]
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#id1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$Foo_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#id2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$Foo_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          }
        ]
      },
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
    ]
  }
}
}