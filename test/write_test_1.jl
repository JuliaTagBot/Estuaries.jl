using JLD
using DataTables
using Gradines

nrows = 10

filename = "testfile.jld"
isfile(filename) && rm(filename)

f = jldopen(filename, "w")

g = Gradine(f)

g[:y1] = rand(nrows)
g[:y2] = 5
g[:y3] = [randstring(5) for i ∈ 1:nrows]
g[:y4] = NullableArray(rand(nrows))
g[:y5] = [DateTime() + Dates.Day(i) for i ∈ 1:nrows]


