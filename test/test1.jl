using HDF5
using Gradines

filename = "testfile.h5"
isfile(filename) && rm(filename)

f = h5open(filename, "w")

g = Gradine(f)

g[:y1] = rand(10)
g[:y2] = 5
g[:y3] = "strings"



