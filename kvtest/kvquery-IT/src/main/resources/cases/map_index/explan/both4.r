compiled-query-plan

{
"query file" : "map_index/q/both4.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0 ],
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
      "index row variable" : "$$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"rec.a":10},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "ANY_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec.c.vAlues().ca",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 1
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "ANY_GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "KEYS",
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
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "c1"
          }
        },
        {
          "iterator kind" : "ANY_GREATER_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "KEYS",
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
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "c1"
          }
        }
      ]
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
      }
    ]
  }
}
}