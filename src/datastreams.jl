#=========================================================================================
    datastreams.jl

    This is the datastreams interface for Estuaries.  Basically just re-implements
    the interface for its source.
=========================================================================================#

#=========================================================================================
    <source interface>
=========================================================================================#
Data.schema(E::Estuary) = E.schema
schema(E::Estuary) = Data.schema(E)
schema!(E::Estuary) = (E.schema = Data.schema(E.src))
export schema, schema!

Data.isdone(E::Estuary, row, col) = Data.isdone(E.src, row, col)
Data.reference(E::Estuary) = Data.reference(E.src)

# size is implemented in the Base interface section in gradine.jl

Data.schema(E::Estuary, ::Type{Data.Field}) = Data.schema(E.src, Data.Field)

Data.streamtype(E::Estuary, ::Type{Data.Field}) = Data.streamtype(E.src, Data.Field)

# this for both regular and nullable types
function Data.streamfrom{T}(E::Estuary, ::Type{Data.Field}, ::Type{T}, row, col)
    Data.streamfrom(E.src, Data.Field, T, row, col)
end


Data.schema(E::Estuary, ::Type{Data.Column}) = Data.schema(E.src, Data.Column)

Data.streamtype(E::Estuary, ::Type{Data.Column}) = Data.streamtype(E.src, Data.Column)

# this for both regular and nullable types
function Data.streamfrom{T}(E::Estuary, ::Type{Data.Column}, ::Type{T}, col)
    Data.streamfrom(E.src, Data.Column, T, col)
end
#=========================================================================================
    </source interface>
=========================================================================================#


#=========================================================================================
    <sink interface>
=========================================================================================#
Data.streamtypes(E::Estuary) = Data.streamptypes(sink)

# does this need special constructors???

function Data.streamto!(E::Estuary, ::Type{Data.Field}, val, row, col)
    Data.streamto!(E.sink, Data.Field, val, row, col)
end
function Data.streamto!(E::Estuary, ::Type{Data.Field}, val, row, col, schema::Data.Schema)
    Data.streamto!(E.sink, Data.Field, val, row, col, schema)
end

function Data.streamto!(E::Estuary, ::Type{Data.Column}, v, row, col)
    Data.streamto!(E.sink, Data.Column, v, row, col)
end
function Data.streamto!(E::Estuary, ::Type{Data.Column}, v, row, col, schema::Data.Schema)
    Data.streamto!(E.sink, Data.Column, v, row, col, schema)
end

Data.cleanup!(E::Estuary) = Data.cleanup!(E.sink)
Data.close!(E::Estuary) = Data.close!(E.sink)
#=========================================================================================
    </sink interface>
=========================================================================================#


#=========================================================================================
    <schema extensions>  consider adding to DataStreams.jl

    Note that usually objects with the DataStreams interface do not support operations
    that change the schema. That was probably the original intention.
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
    push!(schema.header, name)
    push!(schema.types, T)
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



