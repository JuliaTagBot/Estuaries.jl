
const ColumnIndex = Union{Integer, Symbol}


"""
# Type `Estuary`

This type acts as a wrapper for any tabular data with the `DataStreams` interface that
behaves like a regular dataframe.  It can have either a source, sink or both defined.
If a source is defined, the `Estuary` can be used to read data.  If a sink is defined, it
can be used to write data.  If both are defined, it is assumed that both represent the same
dataset, so that these both share a schema.

Most attributes are compted from the source by default.
"""
struct Estuary <: AbstractEstuary
    src::Any  # this must be something with a Data.Source interface, void if empty
    sink::Any  # this must be something with a Data.Sink interface, void if empty

    schema::Data.Schema

    Estuary(src, sink, schema::Data.Schema) = new(src, sink, schema)
    Estuary(src, sink) = new(src, sink, Data.schema(src))
    Estuary(srcsink) = new(srcsink, srcsink, Data.schema(srcsink))
end
export Estuary


#=========================================================================================
    <interface>
=========================================================================================#
source(E::Estuary) = E.src
sink(E::Estuary) = E.sink
export source, sink

hassource(E::Estuary) = !(E.src isa Void)
hassink(E::Estuary) = !(E.sink isa Void)
export hassource, hassink

Base.size(E::Estuary) = size(E.schema)
DataFrames.ncol(E::Estuary) = size(E.schema,2)
DataFrames.nrow(E::Estuary) = size(E.schema,1)
Base.size(E::Estuary, idx::Integer) = size(E.schema, idx)
Base.names(E::Estuary) = Symbol.(Data.header(E.schema))

Base.copy(E::Estuary) = Estuary(copy(E.src), Data.schema(E.src))

Base.deepcopy(E::Estuary) = Estuary(deepcopy(E.src), deepcopy(E.schema))

Base.eltype(E::Estuary, col::Integer) = Data.types(E.schema)[col]
Base.eltype(E::Estuary, col::String) = eltype(E, E.schema[col])
Base.eltype(E::Estuary, col::Symbol) = eltype(E, string(col))

DataFrames.eltypes(E::Estuary) = Data.types(E.schema)
function DataFrames.eltypes{T<:Union{Integer, Symbol, String}}(E::Estuary,
                                                               v::AbstractVector{T})
    Type[eltype(E, c) for c ∈ v]
end
export eltypes

function Base.show(io::IO, E::Estuary)
    println("Estuary $(size(E)); source defined: $(hassource(E)); sink defined: $(hassink(E))")
    show(E.schema)
end

# these are needed for streamfrom for columns
_vector_type{T}(::Type{T}) = Vector{T}
_vector_type{T}(::Type{Nullable{T}}) = NullableVector{T}
function vector_type{T<:Union{Integer,Symbol,String}}(E::Estuary, col::T)
    _vector_type(eltype(E, col))
end

vector_types(E::Estuary) = Type[vector_type(E,c) for c ∈ 1:size(E,2)]
function vector_types{T<:Union{Integer,Symbol,String}}(E::Estuary, v::AbstractVector{T})
    Type[vector_type(E,c) for c ∈ v]
end
#=========================================================================================
    </interface>
=========================================================================================#


#=========================================================================================
    <getindex>
    TODO make sure these can take nullable vector indices
=========================================================================================#
function _get_partial_col{T}(E::Estuary, ::Type{T}, row_inds::AbstractVector{<:Integer},
                             ncol::Integer)
    Vector{T}([Data.streamfrom(E, Data.Field, T, i, ncol) for i ∈ row_inds])
end
function _get_partial_col{T}(E::Estuary, ::Type{Nullable{T}},
                             row_inds::AbstractVector{<:Integer},
                             ncol::Integer)
    NullableArray{T}([Data.streamfrom(E, Data.Field, Nullable{T}, i, ncol) for i ∈ row_inds])
end


# E[SingleColumnIndex] ⇒ AbstractVector
function getindex(E::Estuary, col_ind::ColumnIndex)
    ncol = E.schema[col_ind]
    Data.streamfrom(E, Data.Column, eltype(E, ncol), ncol)
end

# E[MultiColumnIndex] ⇒ DataFrame
function getindex{T<:ColumnIndex}(E::Estuary, col_inds::AbstractVector{T})
    ncols = E.schema[col_inds]
    dtypes = vector_types(E, ncols)
    cols = Any[Data.streamfrom(E, Data.Column, dtypes[i], ncols[i]) for i ∈ 1:length(ncols)]
    DataFrame(cols, DataFrames.Index(names(E)[ncols]))
end

# E[:] ⇒ Estuary
getindex(E::Estuary, colon::Colon) = copy(E)

# E[SingleRowIndex, SingleColumnIndex] ⇒ Scalar
function getindex(E::Estuary, row_ind::Integer, col_ind::ColumnIndex)
    ncol = E.schema[col_ind]
    dtype = eltype(E, ncol)
    Data.streamfrom(E, Data.Field, dtype, row_ind, ncol)
end

# E[SingleRowIndex, MultiColumnIndex] ⇒ DataFrame
function getindex{T<:ColumnIndex}(E::Estuary, row_ind::Integer, col_inds::AbstractVector{T})
    ncols = E.schema[col_inds]
    dtypes = eltypes(E, ncols)
    cols = Vector{Any}(length(ncols))
    for i ∈ 1:length(ncols)
        cols[i] = Vector{dtypes[i]}([Data.streamfrom(E, Data.Field, dtypes[i],
                                                     row_ind, ncols[i])])
    end
    DataFrame(cols, DataFrames.Index(names(E)[ncols]))
end

# E[MultiRowIndex, SingleColumnIndex] ⇒ AbstractVector
function getindex{T<:Integer}(E::Estuary, row_inds::AbstractVector{T}, col_ind::ColumnIndex)
    ncol = E.schema[col_ind]
    dtype = eltype(E, ncol)
    _get_partial_col(E, dtype, row_inds, ncol)
end

# E[MultiRowIndex, MultiColumnIndex] ⇒ DataFrame
function getindex{R<:Integer, T<:ColumnIndex}(E::Estuary,
                                              row_inds::AbstractVector{R},
                                              col_inds::AbstractVector{T})
    ncols = E.schema[col_inds]
    dtypes = eltypes(E, ncols)
    cols = Any[_get_partial_col(E, dtypes[i], row_inds, ncols[i]) for i ∈ 1:length(ncols)]
    DataFrame(cols, DataFrames.Index(names(E)[ncols]))
end

# E[:, SingleColumnIndex] ⇒ Vector
# E[:, MultiColumnIndex] ⇒ DataFrame
getindex(E::Estuary, ::Colon, col_ind::ColumnIndex) = E[col_ind]
getindex(E::Estuary, ::Colon, col_inds::AbstractVector{<:ColumnIndex}) = E[col_inds]

# E[SingleRowIndex, :] ⇒ DataFrame
getindex(E::Estuary, row_ind::Integer, col_inds::Colon) = E[[row_ind], col_inds]

# E[MultiRowIndex, :] ⇒ DataFrame
function getindex{R<:Integer}(E::Estuary, row_inds::AbstractVector{R}, col_inds::Colon)
    getindex(E, row_inds, 1:size(E,2))
end

# E[:, :] ⇒ Estuary
getindex(E::Estuary, ::Colon, ::Colon) = copy(E)
#=========================================================================================
    </getindex>
=========================================================================================#


#=========================================================================================
    <setindex>
=========================================================================================#
isnextcol(E::Estuary, col_ind::Symbol) = true
function isnextcol(E::Estuary, col_ind::Integer)
    Int64(size(E,2) + 1) == Int64(col_ind)
end
function nextcolname(E::Estuary)
    Symbol(string("x", size(E,2)+1))
end

function upgrade_scalar_nonull(E::Estuary, v::Any)
    n = (size(E,2) == 0) ? 1 : size(E,1)
    fill(v, n)
end


# helper functions for insert_single_column!
function _insert_new_single_column!{T}(E::Estuary, v::AbstractVector{T}, col_ind::Symbol)
    Data.streamto!(E, Data.Column, v, 0, size(E,2)+1, E.schema)
    appendcolumn!(E.schema, string(col_ind), T)
end
function _insert_new_single_column!{T}(E::Estuary, v::AbstractVector{T}, col_ind::Integer)
    if isnextcol(E, col_ind)
        name = nextcolname(E)
        Data.streamto!(E, Data.Column, v, 0, size(E,2)+1, E.schema)
        appendcolumn!(E.schema, string(name), T)
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind."))
    end
end
function _insert_existing_single_column!{T}(E::Estuary, v::AbstractVector{T},
                                            col_ind::Symbol)
    j = E.schema[col_ind]
    Data.streamto!(E, Data.Column, v, 0, j, E.schema)
    altercolumn!(E.schema, j, string(col_ind), T)
end
function _insert_existing_single_column!{T}(E::Estuary, v::AbstractVector{T},
                                            col_ind::Integer)
    n = names(E)[col_ind]
    Data.streamto!(E, Data.Column, v, 0, col_ind, E.schema)
    altercolumn!(E.schema, col_ind, string(n), T)
end


function insert_single_column!(E::Estuary, v::AbstractVector, col_ind::ColumnIndex)
    if E.sink isa Void
        error("This Estuary's data sink is not defined. It doesn't support assignments.")
    end
    if size(E,2) ≠ 0 && size(E,1) ≠ length(v)
        throw(ArgumentError("New columns must have same length as old columns."))
    end
    if haskey(E.schema, col_ind)
        _insert_existing_single_column!(E, v, col_ind)
    else
        _insert_new_single_column!(E, v, col_ind)
    end
    v
end
function insert_single_column!(E::Estuary, v::Any, col_ind::ColumnIndex)
    insert_single_column!(E, upgrade_scalar_nonull(E, v), col_ind)
end


function insert_single_entry!(E::Estuary, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    if E.sink isa Void
        error("This Estuary's data sink is not defined. It doesn't support assignments.")
    end
    if haskey(E.schema, col_ind)
        j = E.schema[col_ind]
        Data.streamto!(E, Data.Field, v, row_ind, j, E.schema)
    else
        throw(ArgumentError("Cannot assign to non-existent column $col_ind."))
    end
end


function insert_multiple_entries!(E::Estuary, v::AbstractVector,
                                  row_inds::AbstractVector{<:Integer},
                                  col_ind::ColumnIndex)
    if E.sink isa Void
        error("This Estuary's data sink is not defined. It doesn't support assignments.")
    end
    if haskey(E.schema, col_ind)
        j = E.schema[col_ind]
        for i ∈ 1:length(v)
            Data.streamto!(E, Data.Field, v[i], row_inds[i], j, E.schema)
        end
    else
        throw(ArgumentError("Cannot assign to non-existent column $col_ind."))
    end
end
function insert_multiple_entries!(E::Estuary, v::Any, row_inds::AbstractVector{<:Integer},
                                  col_ind::ColumnIndex)
    insert_multiple_entries!(E, upgrade_scalar_nonull(E, v), row_inds, col_ind)
end


# E[SingleColumnIndex] = Vector
function Base.setindex!(E::Estuary, v::AbstractVector, col_ind::ColumnIndex)
    insert_single_column!(E, v, col_ind)
end

# E[SingleColumnIndex] = (single item, expands to nrow(E) if ncol(E) > 0)
function Base.setindex!(E::Estuary, v, col_ind::ColumnIndex)
    insert_single_column!(E, upgrade_scalar_nonull(E, v), col_ind)
end

# E[MultiColumnIndex] = DataFrame
function Base.setindex!(E::Estuary, data::DataFrame, col_inds::AbstractVector{<:ColumnIndex})
    for j ∈ 1:length(col_inds)
        insert_single_column!(E, data[j], col_inds[j])
    end
    E
end
function Base.setindex!(E::Estuary, data::DataFrame, col_inds::AbstractVector{Bool})
    setindex!(E, data, find(col_inds))
end

# E[MultiColumnIndex] = AbstractVector (repeated for each column)
function Base.setindex!(E::Estuary, v::AbstractVector,
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind ∈ col_inds
        E[col_ind] = dv
    end
end
function Base.setindex!(E::Estuary, v::AbstractVector, col_inds::AbstractVector{Bool})
    setindex!(E, v, find(col_inds))
end

# E[MultiColumnIndex] = single item (repated for each column)
function Base.setindex!(E::Estuary, v::Any, col_inds::AbstractVector{<:ColumnIndex})
    for col_ind ∈ col_inds
        E[col_ind] = val
    end
    E
end
function Base.setindex!(E::Estuary, v::Any, col_inds::AbstractVector{Bool})
    setindex!(E, v, find(col_inds))
end

# E[:] = AbstractVector or single item
Base.setindex!(E::Estuary, v, ::Colon) = (E[1:size(E,2)] = v; E)

# E[SingleRowIndex, SingleColumnIndex] = single item
function Base.setindex!(E::Estuary, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    insert_single_entry!(E, v, row_ind, col_ind)
end

# E[SingleRowIndex, MultiColumnIndex] = single item
function Base.setindex!(E::Estuary, v::Any, row_ind::Real,
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind ∈ col_inds
        insert_single_entry!(E, v, row_ind, col_ind)
    end
    E
end
function Base.setindex!(E::Estuary, v::Any, row_ind::Real, col_inds::AbstractVector{Bool})
    setindex!(E, v, row_ind, find(col_inds))
end

# E[SingleRowIndex, MultiColumnIndex] = 1-row DataFrame
function Base.setindex!(E::Estuary, data::DataFrame, row_ind::Integer,
                        col_inds::AbstractVector{<:ColumnIndex})
    for j ∈ 1:length(col_inds)
        insert_single_entry!(E, data[j][1], row_ind, col_inds[j])
    end
    E
end
function Base.setindex!(E::Estuary, data::DataFrame, row_ind::Integer,
                        col_inds::AbstractVector{Bool})
    setindex!(E, data, row_ind, find(col_inds))
end

# E[MultiRowIndex, SingleColumnIndex] = AbstractVector
function Base.setindex!(E::Estuary, v::AbstractVector, row_inds::AbstractVector{<:Integer},
                        col_ind::ColumnIndex)
    insert_multiple_entries!(E, v, row_inds, col_ind)
    E
end
function Base.setindex!(E::Estuary, v::AbstractVector, row_inds::AbstractVector{Bool},
                        col_ind::ColumnIndex)
    setindex!(E, v, find(row_inds), col_ind)
end

# E[MultiRowIndex, SingleColumnIndex] = single item
function Base.setindex!(E::Estuary, v::Any, row_inds::AbstractVector{<:Integer},
                        col_ind::ColumnIndex)
    insert_multiple_entries!(E, v, row_inds, col_ind)
    E
end
function Base.setindex!(E::Estuary, v::Any, row_inds::AbstractVector{Bool},
                        col_ind::ColumnIndex)
    setindex!(E, v, find(row_inds), col_ind)
end

# E[MultiRowIndex, MultiColumnIndex] = DataFrame
function Base.setindex!(E::Estuary, data::DataFrame, row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{<:ColumnIndex})
    for j ∈ 1:length(col_inds)
        insert_multiple_entries!(E, data[:, j], row_inds, col_inds[j])
    end
    E
end
function Base.setindex!(E::Estuary, data::DataFrame, row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{Bool})
    setindex!(E, data, row_inds, find(col_inds))
end
function Base.setindex!(E::Estuary, data::DataFrame, row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(E, data, find(row_inds), col_inds)
end
function Base.setindex!(E::Estuary, data::DataFrame, row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(E, data, find(row_inds), find(col_inds))
end

# E[MultiRowIndex, MultiColumnIndex] = AbstractVector
function Base.setindex!(E::Estuary, v::AbstractVector, row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind ∈ col_inds
        insert_multiple_entries!(E, v, row_inds, col_ind)
    end
    E
end
function Base.setindex!(E::Estuary, v::AbstractVector, row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{Bool})
    setindex!(E, v, row_inds, find(col_inds))
end
function Base.setindex!(E::Estuary, v::AbstractVector, row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(E, v, find(row_inds), col_inds)
end
function Base.setindex!(E::Estuary, v::AbstractVector, row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(E, v, find(row_inds), find(col_inds))
end

# E[MultiRowIndex, MultiColumnIndex] = single item
function Base.setindex!(E::Estuary, v::Any, row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind ∈ col_inds
        insert_multiple_entries!(E, v, row_inds, col_ind)
    end
    E
end
function Base.setindex!(E::Estuary, v::Any, row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{Bool})
    setindex!(E, v, row_inds, find(col_inds))
end
function Base.setindex!(E::Estuary, v::Any, row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(E, v, find(row_inds), col_inds)
end
function Base.setindex!(E::Estuary, v::Any, row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(E, v, find(row_inds), find(col_inds))
end

# E[:] = DataFrame; E[:, :] = DataFrame
function Base.setindex!(E::Estuary, data::DataFrame, row_inds::Colon,
                        col_inds::Colon=Colon())
    for n ∈ names(data)
        E[n] = data[n]
    end
    E
end

# E[:, :] = ...
function Base.setindex!(E::Estuary, v, ::Colon, ::Colon)
    E[1:size(E,1), 1:size(E,2)] = v
    E
end

# E[Any, :] = ...
function Base.setindex!(E::Estuary, v, row_inds, ::Colon)
    E[row_inds, 1:size(E,2)] = v
    E
end

# E[:, Any]
Base.setindex!(E::Estuary, v, ::Colon, col_inds) = (E[col_inds] = v; E)

# TODO I don't think the DataStreams interface supports this
# special deletion assignment
Base.setindex!(E::Estuary, ::Void, col_ind::Integer) = delete!(E, col_ind)
#=========================================================================================
    </setindex>
=========================================================================================#


#=========================================================================================
    <constructors>
=========================================================================================#
Source(src) = Estuary(src, nothing)
Sink(sink) = Estuary(nothing, sink)

# note that `Estuary`s lack special Sink constructors, since they must themselves be
# created from a data source or sink
#=========================================================================================
    </constructors>
=========================================================================================#


#=========================================================================================
    <accessors>
=========================================================================================#
DataFrames.head(E::Estuary, nrows::Integer=6) = E[1:nrows, :]
DataFrames.tail(E::Estuary, nrows::Integer=6) = E[(end-nrows):end, :]

Base.convert(::Type{DataFrame}, E::Estuary) = E[1:size(E,1), 1:size(E,2)]

export head, tail
#=========================================================================================
    </accessors>
=========================================================================================#




