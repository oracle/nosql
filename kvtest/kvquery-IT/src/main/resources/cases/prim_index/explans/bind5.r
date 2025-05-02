compiled-query-plan

{
"query file" : "prim_index/q/bind5.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "primary key bind expressions" : [
    null,
    {
      "iterator kind" : "EXTERNAL_VAR_REF",
      "variable" : "$ext5_1"
    },
    {
      "iterator kind" : "EXTERNAL_VAR_REF",
      "variable" : "$ext5_2"
    },
    null
  ],
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
          "equality conditions" : {"id1":3,"id2":0.0,"id3":"tok1","id4":"id4-3"},
          "range conditions" : {}
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$ext5_1"
        },
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$ext5_2"
        }
      ],
      "map of key bind expressions" : [
        [ -1, 0, 1, -1 ]
      ],
      "index filtering predicate" :
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
          "iterator kind" : "CONST",
          "value" : 50.0
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