compiled-query-plan

{
"query file" : "map_index/q/in08.q",
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
      "index used" : "idx4_c1_keys_vals_c3",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"rec.c.c1.ca":3,"rec.c.Keys()":"c4"},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"rec.c.c1.ca":10,"rec.c.Keys()":"c4"},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"rec.c.c1.ca":3,"rec.c.Keys()":"c3"},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"rec.c.c1.ca":10,"rec.c.Keys()":"c3"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
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
            "variable" : "$$f_idx"
          }
        }
      }
    ]
  }
}
}