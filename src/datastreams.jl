#=========================================================================================
    datastreams.jl

    This is the datastreams interface for gradines.  Basically just re-implements
    the interface for its source.
=========================================================================================#

#=========================================================================================
    <source interface>
=========================================================================================#
Data.schema(g::Gradine) = g.schema
schema(g::Gradine) = Data.schema(g)
schema!(g::Gradine) = (g.schema = Data.schema(src))
export schema, schema!

Data.isdone(g::Gradine, row, col) = Data.isdone(g.src, row, col)
Data.reference(g::Gradine) = Data.reference(g.src)

# size is implemented in the Base interface section in gradine.jl

Data.schema(g::Gradine, ::Type{Data.Field}) = Data.schema(g.src, Data.Field)

Data.streamtype(g::Gradine, ::Type{Data.Field}) = Data.streamtype(g.src, Data.Field)

# this for both regular and nullable types
function Data.streamfrom{T}(g::Gradine, ::Type{Data.Field}, ::Type{T}, row, col)
    Data.streamfrom(g.src, Data.Field, T, row, col)
end


Data.schema(g::Gradine, ::Type{Data.Column}) = Data.schema(g.src, Data.Column)

Data.streamtype(g::Gradine, ::Type{Data.Column}) = Data.streamtype(g.src, Data.Column)

# this for both regular and nullable types
function Data.streamfrom{T}(g::Gradine, ::Type{Data.Column}, ::Type{T}, col)
    Data.streamfrom(g.src, Data.Column, T, col)
end
#=========================================================================================
    </source interface>
=========================================================================================#


#=========================================================================================
    <sink interface>
=========================================================================================#
Data.streamtypes(g::Gradine) = Data.streamptypes(sink)

# does this need special constructors???

function Data.streamto!(g::Gradine, ::Type{Data.Field}, val, row, col)
    Data.streamto!(g.sink, Data.Field, val, row, col)
end
function Data.streamto!(g::Gradine, ::Type{Data.Field}, val, row, col, schema::Data.Schema)
    Data.streamto!(g.sink, Data.Field, val, row, col, schema)
end

function Data.streamto!(g::Gradine, ::Type{Data.Column}, v, row, col)
    Data.streamto!(g.sink, Data.Column, v, row, col)
end
function Data.streamto!(g::Gradine, ::Type{Data.Column}, v, row, col, schema::Data.Schema)
    Data.streamto!(g.sink, Data.Column, v, row, col, schema)
end

Data.cleanup!(g::Gradine) = Data.cleanup!(g.sink)
Data.close!(g::Gradine) = Data.close!(g.sink)
#=========================================================================================
    </sink interface>
=========================================================================================#


#=========================================================================================
    <schema extensions>  consider adding to DataStreams.jl
=========================================================================================#
function altercolumn!{T}(schema::Data.Schema, idx::Integer, name::String, ::Type{T})
    schema.header[idx] = name
    schema.types[idx] = T
    schema
end

function insertcolumn!{T}(schema::Data.Schema, idx::Integer, name::String, ::Type{T})
    insert!(schema.header, idx, name)
    insert!(schema.types, idx, T)
    schema.cols += 1
    schema.index = Dict(n=>i for (i, n) ∈ enumerate(schema.header))
    schema
end

function appendcolumn!{T}(schema::Data.Schema, name::String, ::Type{T})
    append!(schema.header, name)
    append!(schema.types, T)
    schema.cols += 1
    schema.index[name] = schema.cols
    schema
end

function deletecolumn!(schema::Data.Schema, idx::Integer)
    name = splice!(schema.header, idx)
    deleteat!(schema.types, idx)
    schema.cols -= 1
    delete!(schema.index, name)
    schema
end

function deletecolumn!(schema::Data.Schema, name::String)
    idx = find(s -> s == name, schema.header)
    deleteat!(schema.header, idx)
    deleteat!(schema.types, idx)
    schema.cols -= 1
    delete!(schema.index, name)
    schema
end
#=========================================================================================
    </schema extensions>
=========================================================================================#



