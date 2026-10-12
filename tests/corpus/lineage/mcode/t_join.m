let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    other = Sql.Database("srv","erp"){[Schema="dbo",Item="U"]}[Data],
    Result = Table.Join(Source, "id", other, "uid")
in
    Result
