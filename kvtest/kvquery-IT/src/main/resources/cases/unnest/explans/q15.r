compiled-query-plan

{
"query file" : "unnest/q/q15.q",
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
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$t",
    "FROM" :
    {
      "iterator kind" : "ARRAY_FILTER",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "phones",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "address",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$t"
          }
        }
      }
    },
    "FROM variable" : "$phone",
    "FROM" :
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "children",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$t"
      }
    },
    "FROM variable" : "$children",
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
        "field name" : "Mark",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "Mark",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$children"
          }
        }
      }
    ]
  }
}
}