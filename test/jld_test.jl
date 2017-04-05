using JLD
using HDF5
using DataTables

const filename = "testfile.jld"
const N = 100

f = jldopen(filename, "w")

isfile(filename) && rm(filename)

f["a"] = [randstring(5) for i ∈ 1:N]
f["b"] = rand(N)

g = g_create(f, "c")
g["d"] = [randstring(5) for i ∈ 1:N]


