__precompile__(true)

module Estuaries

using Reexport

using DataTables
@reexport using DataStreams

import Base.convert
import Base: length, size
import Base: getindex, setindex!
import Base.copy
import Base.eltype
import Base.empty!, Base.delete!
import Base: insert!, merge!
import Base.isnull
import Base.show

import DataTables.index
import DataTables.eltypes
import DataTables: ncol, nrow
import DataTables: head, tail

include("abstracts.jl")
include("utils.jl")
include("estuary.jl")
include("datastreams.jl")

end # module
