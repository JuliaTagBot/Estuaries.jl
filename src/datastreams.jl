#=========================================================================================
    datastreams.jl

    This is the gradine DataStreams interface.
    An example implementation of this interface can be found in
    DataTables/src/abstractdatatable/io.jl
=========================================================================================#

function Data.schema(g::Gradine, ::Type{Data.Column})
    Data.Schema(string.(names(g)), typeof.(g.columns), size(g, 1))
end

