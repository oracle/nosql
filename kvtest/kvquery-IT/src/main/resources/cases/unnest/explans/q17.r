compiled-query-plan

{
"query file" : "unnest/q/q17.q",
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
      "target table" : "bar",
      "row variable" : "$b",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$b",
    "FROM" :
    {
      "iterator kind" : "KEYS",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$b"
      }
    },
    "FROM variable" : "$k",
    "FROM" :
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : 
            {
        "iterator kind" : "VAR_REF",
        "variable" : "$k"
      },
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$b"
      }
    },
    "FROM variable" : "$v",
    "SELECT expressions" : [
      {
        "field name" : "b",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$b"
        }
      },
      {
        "field name" : "k",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$k"
        }
      },
      {
        "field name" : "v",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$v"
        }
      }
    ]
  }
}
}