let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    Result = Table.DuplicateColumn(Source, "a", "a2")
in
    Result
