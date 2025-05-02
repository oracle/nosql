compiled-query-plan

{
"query file" : "map_index/q/in11.q",
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
      "target table" : "Foo",
      "row variable" : "$$t",
      "index used" : "idx1_a_c_c_f",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"rec.a":10,"rec.c.keys()":"c1","rec.c.vAlues().ca":3},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"rec.a":10,"rec.c.keys()":"c1","rec.c.vAlues().ca":11},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "ca",
        "input iterator" :
        {
          "iterator kind" : "VALUES",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "rec",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 3
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "g",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "g",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      }
    ]
  }
}
}