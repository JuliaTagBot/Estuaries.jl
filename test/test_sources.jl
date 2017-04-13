using DataTables
using Estuaries
using Feather
using DataUtils
import DataFrames

const nrows = 10

function makefeather(filename::String, data::DataTable)
    Feather.write(filename, convert(DataFrames.DataFrame, data))
end

# data = DataTable(A=rand(nrows), B=[randstring(5) for i ∈ 1:nrows])
# makefeather("testfile.feather", data)

E = Estuary(Feather.Source("testfile.feather"), Feather.Sink("testfile.feather"))


