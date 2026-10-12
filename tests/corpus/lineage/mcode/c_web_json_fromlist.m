let Source = Json.Document(Web.Contents("https://api.example.com/v1/items?key=SECRET")), t = Table.FromList(Source, Splitter.SplitByNothing()) in t
