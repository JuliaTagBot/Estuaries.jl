__precompile__(true)

module Gradines

#= TODO
    Implementing strings in HDF5 is a huge pain in the ass.  One alternative is to use JLD
    which has done this already, but at the cost of losing easy compatibility with python.

    For now the size of the Gradine will have to be declared when it is first built.
=#


using DataTables
using DataStreams
# using DataUtils
using JLD
using HDF5

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

1. Create Gradine and GradineColumns.
2. Better support for strings.
3. Accomodate Date and DateTime types.
3. Create NullableGradineColumns.
4. Create conversions to and from DataTables.
5. Good methods for accessing pieces at a time.
6. Most features of datatables.
7. Consider inheriting from AbstractDataTable

=#


include("abstracts.jl")
include("utils.jl")
include("gradinecolumn.jl")
include("nullablegradinecolumn.jl")
include("gradine.jl")

end # module
