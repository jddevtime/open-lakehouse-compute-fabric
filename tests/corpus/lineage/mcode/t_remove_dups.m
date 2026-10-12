let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    Result = Table.Distinct(Source, {"a"})
in
    Result
