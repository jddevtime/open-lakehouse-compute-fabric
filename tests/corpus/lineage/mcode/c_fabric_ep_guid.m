let
 S = Sql.Database("abc.datawarehouse.fabric.microsoft.com", "33333333-3333-4333-8333-333333333333"),
 t = S{[Schema="dbo",Item="sales"]}[Data]
in
 t
