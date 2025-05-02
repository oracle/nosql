compiled-query-plan

{
"query file" : "idc_unnest_json/q/map03.q",
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
      "target table" : "User_json",
      "row variable" : "$u",
      "index used" : "idx_children_values",
      "covering index" : true,
      "index row variable" : "$u_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
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
              "field name" : "info.children.values().school",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$u_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : "sch_1"
            }
          },
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.children.values().school",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$u_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : "sch_3"
            }
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$u_idx",
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
            "variable" : "$u_idx"
          }
        }
      },
      {
        "field name" : "age",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.children.values().age",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$u_idx"
          }
        }
      }
    ]
  }
}
}