compiled-query-plan

{
"query file" : "prim_index_sort/q/sort6.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 4 ],
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "SINGLE_PARTITION",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$Foo",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"id1":0,"id2":1},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$Foo",
      "SELECT expressions" : [
        {
          "field name" : "id1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo"
            }
          }
        },
        {
          "field name" : "id2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo"
            }
          }
        },
        {
          "field name" : "id3",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo"
            }
          }
        },
        {
          "field name" : "id4",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id4",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo"
            }
          }
        },
        {
          "field name" : "lastName",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "lastName",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Foo"
            }
          }
        }
      ]
    }
  }
}
}