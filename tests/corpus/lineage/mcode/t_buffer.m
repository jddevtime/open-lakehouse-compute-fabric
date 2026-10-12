let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    Result = Table.Buffer(Source)
in
    Result
