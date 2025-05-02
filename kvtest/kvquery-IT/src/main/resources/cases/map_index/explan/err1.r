compiled-query-plan

{
"query file" : "map_index/q/err1.q",
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
          "equality conditions" : {"rec.a":10,"rec.c.keys()":"c1"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
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
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
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
            }
          ]
        }
      }
    ]
  }
}
}