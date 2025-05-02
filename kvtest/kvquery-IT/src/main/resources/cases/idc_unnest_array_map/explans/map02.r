compiled-query-plan

{
"query file" : "idc_unnest_array_map/q/map02.q",
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
      "target table" : "User",
      "row variable" : "$u",
      "index used" : "idx_children_both",
      "covering index" : false,
      "index row variable" : "$u_idx",
      "index scans" : [
        {
          "equality conditions" : {"children.keys()":"Anna"},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "GREATER_THAN",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "children.values().school",
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
      "position in join" : 0
    },
    "FROM variable" : "$u",
    "FROM" :
    {
      "iterator kind" : "VALUES",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "children",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$u"
        }
      }
    },
    "FROM variable" : "$child_info",
    "WHERE" : 
    {
      "iterator kind" : "GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "age",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$child_info"
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 10
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
            "variable" : "$u"
          }
        }
      },
      {
        "field name" : "friends",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "friends",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$child_info"
          }
        }
      }
    ]
  }
}
}