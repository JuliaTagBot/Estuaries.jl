using DataTables
using Estuaries

const nrows = 10

data = DataTable(A=rand(nrows), B=[randstring(5) for i ∈ 1:nrows])

E = Estuary(data)


