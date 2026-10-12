let Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data], x = Foo.Transform(Source, 1) in x
