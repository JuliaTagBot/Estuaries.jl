__precompile__(true)

module Estuaries

using Reexport

using DataFrames
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
import Base.haskey

import DataFrames.index
import DataFrames.eltypes
import DataFrames: ncol, nrow
import DataFrames: head, tail

include("abstracts.jl")
include("utils.jl")
include("estuary.jl")
include("datastreams.jl")

end # module
