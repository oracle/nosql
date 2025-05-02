compiled-query-plan

{
"query file" : "unnest/q/q18.q",
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
      "row variable" : "$t",
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
    "FROM variable" : "$t",
    "FROM" :
    {
      "iterator kind" : "KEYS",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$t"
      }
    },
    "FROM variable" : "$k",
    "SELECT expressions" : [
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
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "VALUES",
              "predicate iterator" :
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$key"
                },
                "right operand" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$k"
                }
              },
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$t"
              }
            }
          ]
        }
      }
    ]
  }
}
}