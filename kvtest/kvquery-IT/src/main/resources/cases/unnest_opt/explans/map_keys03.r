compiled-query-plan

{
"query file" : "unnest_opt/q/map_keys03.q",
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
      "row variable" : "$t",
      "index used" : "idx_children_both",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"children.keys()":"Anna"},
          "range conditions" : { "children.values().age" : { "start value" : 5, "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$t",
    "FROM" :
    {
      "iterator kind" : "KEYS",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "children",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$t"
        }
      }
    },
    "FROM variable" : "$child",
    "FROM" :
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "Anna",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "children",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$t"
        }
      }
    },
    "FROM variable" : "$child_info",
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
            "variable" : "$t"
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