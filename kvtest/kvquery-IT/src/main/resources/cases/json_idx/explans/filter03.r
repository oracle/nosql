compiled-query-plan

{
"query file" : "json_idx/q/filter03.q",
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
      "row variable" : "$$f",
      "index used" : "idx_children_values",
      "covering index" : false,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "info.children.values().age" : { "end value" : 10, "end inclusive" : true } }
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "ANY_LESS_OR_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.children.values().school",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "sch_1"
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "ANY_LESS_OR_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "school",
        "input iterator" :
        {
          "iterator kind" : "VALUES",
          "predicate iterator" :
          {
            "iterator kind" : "AND",
            "input iterators" : [
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$key"
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : "Anna"
                }
              },
              {
                "iterator kind" : "LESS_OR_EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "age",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$value"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              }
            ]
          },
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "children",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "sch_1"
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
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}