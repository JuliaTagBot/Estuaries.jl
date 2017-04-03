__precompile__(true)

module Gradines

using DataTables
# using DataUtils
using HDF5

import Base.convert
import Base.length
import Base.size
import Base.getindex
import Base.setindex!
import Base.copy
import Base.eltype

import DataTables.index
import DataTables.eltypes
import DataTables.ncol
import DataTables.nrow

import HDF5.name

#= TODO:

1. Create Gradine and GradineColumns.
2. Better support for strings.
3. Accomodate Date and DateTime types.
3. Create NullableGradineColumns.
4. Create conversions to and from DataTables.
5. Good methods for accessing pieces at a time.
6. Most features of datatables.

=#


include("abstracts.jl")
include("gradine.jl")
include("gradinecolumn.jl")
include("utils.jl")

end # module
