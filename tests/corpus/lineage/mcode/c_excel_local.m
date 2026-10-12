let
 S = Excel.Workbook(File.Contents("C:\\data\\book.xlsx"), null, true),
 t = S{[Item="Sheet1",Kind="Sheet"]}[Data]
in
 t
