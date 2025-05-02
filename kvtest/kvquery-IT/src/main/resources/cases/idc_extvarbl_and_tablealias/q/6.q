# TestDescription: Literal table alias with valid characters.

select id from Users AS true_false_Int_Float_String
where (true_false_Int_Float_String.age > 35 and true_false_Int_Float_String.age < 47) or
      lastName = "Morrison" and firstName > "David"
