using Estuaries
using DataTables
using DataUtils
using Feather

# const filename = "littletest.feather"
const filename = "bigtest.feather"


function maketestfeather(filename::String; nrows::Integer=10^8)
    data = randomData(Int64, Float64, String, DateTime, nrows=nrows)
    featherWrite(filename, data, overwrite=true)
end

# maketestfeather(filename, nrows=100)

s = Feather.Source(filename)
E = Estuaries.Source(s)


