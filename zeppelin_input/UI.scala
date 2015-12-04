val area = z.input("Area Code").toString();
val cat = z.select("Categories" , Seq(("Restaurants","Restaurants"),
                                      ("Shopping","Shopping"),
                                      ("Beauty and Spa","Beauty and Spa"),
                                      ("Health and Medical","Health and Medical"))).toString()

z.put("zipcode",area)
z.put("category", cat)