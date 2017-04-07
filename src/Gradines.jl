__precompile__(true)

module Gradines

#= TODO
    Make Gradines completely generic! 
    Gradines should wrap a Data.Source!
=#


using DataTables
# using JLD
# using HDF5

importall DataStreams

import Base.convert
import Base: length, size
import Base: getindex, setindex!
import Base.copy
import Base.eltype
import Base.empty!, Base.delete!
import Base: insert!, merge!
import Base.isnull

import DataTables.index
import DataTables.eltypes
import DataTables: ncol, nrow
import DataTables: head, tail

import HDF5.name

#= TODO:

4. Create conversions to and from DataTables.
5. Good methods for accessing pieces at a time.
6. Most features of datatables.
7. Consider inheriting from AbstractDataTable

=#

include("abstracts.jl")
include("utils.jl")
include("gradine.jl")
include("datastreams.jl")

end # module
