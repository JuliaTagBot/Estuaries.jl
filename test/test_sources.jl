using DataTables
using Gradines

const nrows = 100

data = DataTable(A=rand(nrows), B=[randstring(5) for i ∈ 1:nrows])

g = Gradine(data)


