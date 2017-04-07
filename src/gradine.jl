
const ColumnIndex = Union{Integer, Symbol}


"""
# Type `Gradine`

This type acts as a wrapper for any tabular data with the `DataStreams` interface that
behaves like a regular dataframe.  It can have either a source, sink or both defined.
If a source is defined, the `Gradine` can be used to read data.  If a sink is defined, it
can be used to write data.  If both are defined, it is assumed that both represent the same
dataset, so that these both share a schema.

Most attributes are compted from the source by default.
"""
struct Gradine <: AbstractGradine
    src::Any  # this must be something with a Data.Source interface, void if empty
    sink::Any  # this must be something with a Data.Sink interface, void if empty

    schema::Data.Schema
    colindex::DataTables.Index  # we keep this around for efficient lookups

    function Gradine(src, sink, schema::Data.Schema, colindex::DataTables.Index)
        new(src, sink, schema, colindex)
    end
    function Gradine(src, sink, schema::Data.Schema)
        new(src, sink, schema, DataTables.Index(schema))
    end
    function Gradine(src, sink)
        new(src, sink, schema, DataTables.Index(schema))
    end
end
export Gradine


#=========================================================================================
    <interface>
=========================================================================================#
source(g::Gradine) = g.src
sink(g::Gradine) = g.sink
export source, sink

hassource(g::Gradine) = !(g.src <: Void)
hassink(g::Gradine) = !(g.sink <: Void)
export hassource, hassink

Base.size(g::Gradine) = size(g.schema)
DataTables.ncol(g::Gradine) = size(g.schema,2)
DataTables.nrow(g::Gradine) = size(g.schema,1)
Base.size(g::Gradine, idx::Integer) = size(g.schema, idx)
DataTables.index(g::Gradine) = g.colindex
index!(g::Gradine) = (g.colindex = DataTables.Index(schema))
Base.names(g::Gradine) = Symbol.(Data.header(g.schema))

Base.copy(g::Gradine) = Gradine(copy(g.src), Data.schema(g.src))

Base.deepcopy(g::Gradine) = Gradine(deepcopy(g.src), deepcopy(g.schema))

Base.eltype(g::Gradine, col::Integer) = Data.types(g.schema)[col]
Base.eltype(g::Gradine, col::Symbol) = eltype(g, index(g)[col])
Base.eltype(g::Gradine, col::String) = eltype(g, Symbol(col))

DataTables.eltypes(g::Gradine) = Data.types(g.schema)
function DataTables.eltypes{T<:Union{Integer, Symbol, String}}(g::Gradine,
                                                               v::AbstractVector{T})
    DataType[eltype(g, c) for c ∈ v]
end
export eltypes
#=========================================================================================
    </interface>
=========================================================================================#


#=========================================================================================
    <getindex>
    TODO make sure these can take nullable vector indices
=========================================================================================#
function _get_partial_col{T}(g::Gradine, ::Type{T}, row_inds::AbstractVector{<:Integer},
                             ncol::Integer)
    Vector{T}([Data.streamfrom(g, Data.Field, T, i, ncol) for i ∈ row_inds])
end
function _get_partial_col{T}(g::Gradine, ::Type{Nullable{T}},
                             row_inds::AbstractVector{<:Integer},
                             ncol::Integer)
    NullableArray{T}([Data.streamfrom(g, Data.Field, Nullable{T}, i, ncol) for i ∈ row_inds])
end


# g[SingleColumnIndex] ⇒ AbstractGradineColumn
function getindex(g::Gradine, col_ind::ColumnIndex)
    ncol = index(g)[col_ind]
    Data.streamfrom(g, Data.Column, eltype(g, ncol), ncol)
end

# g[MultiColumnIndex] ⇒ DataTable
function getindex{T<:ColumnIndex}(g::Gradine, col_inds::AbstractVector{T})
    ncols = index(g)[col_inds]
    dtypes = eltypes(g, ncols)
    cols = Any[Data.streamfrom(g, Data.Column, dtypes[i], ncols[i]) for i ∈ 1:length(ncols)]
    DataTable(cols, DataTables.Index(names(g)[ncols]))
end

# g[:] ⇒ Gradine
getindex(g::Gradine, colon::Colon) = copy(g)

# g[SingleRowIndex, SingleColumnIndex] ⇒ Scalar
function getindex(g::Gradine, row_ind::Integer, col_ind::ColumnIndex)
    ncol = index(g)[col_ind]
    dtype = eltype(g, ncol)
    Data.streamfrom(g, Data.Field, dtype, row_ind, ncol)
end

# g[SingleRowIndex, MultiColumnIndex] ⇒ DataTable
function getindex{T<:ColumnIndex}(g::Gradine, row_ind::Integer, col_inds::AbstractVector{T})
    ncols = index(g)[col_inds]
    dtypes = eltypes(g, ncols)
    cols = Vector{Any}(length(ncols))
    for i ∈ 1:length(ncols)
        cols[i] = Vector{dtypes[i]}([Data.streamfrom(g, Data.Field, dtypes[i],
                                                     row_ind, ncols[i])])
    end
    DataTable(cols, DataTables.Index(names(g)[ncols]))
end

# g[MultiRowIndex, SingleColumnIndex] ⇒ AbstractVector
function getindex{T<:Integer}(g::Gradine, row_inds::AbstractVector{T}, col_ind::ColumnIndex)
    ncol = index(g)[col_ind]
    dtype = eltype(g, ncol)
    _get_partial_col(g, dtype, row_inds, ncol)
end

# g[MultiRowIndex, MultiColumnIndex] ⇒ DataTable
function getindex{R<:Integer, T<:ColumnIndex}(g::Gradine,
                                              row_inds::AbstractVector{R},
                                              col_inds::AbstractVector{T})
    ncols = index(g)[col_inds]
    dtypes = eltypes(g, ncols)
    cols = Any[_get_partial_col(g, dtypes[i], row_inds, ncols[i]) for i ∈ 1:length(ncols)]
    DataTable(cols, DataTables.Index(names(g)[ncols]))
end

# g[:, SingleColumnIndex] ⇒ Vector
# g[:, MultiColumnIndex] ⇒ DataTable
getindex(g::Gradine, ::Colon, col_ind::ColumnIndex) = g[col_ind]
getindex{T<:ColumnIndex}(g::Gradine, ::Colon, col_inds::AbstractVector{T}) = g[col_inds]

# g[SingleRowIndex, :] ⇒ DataTable
getindex(g::Gradine, row_ind::Integer, col_inds::Colon) = g[[row_ind], col_inds]

# g[MultiRowIndex, :] ⇒ DataTable
function getindex{R<:Integer}(g::Gradine, row_inds::AbstractVector{R}, col_inds::Colon)
    getindex(g, row_inds, 1:size(g,2))
end

# g[:, :] ⇒ Gradine
getindex(g::Gradine, ::Colon, ::Colon) = copy(g)
#=========================================================================================
    </getindex>
=========================================================================================#


#=========================================================================================
    <setindex>
=========================================================================================#
isnextcol(g::Gradine, col_ind::Symbol) = true
function isnextcol(g::Gradine, col_ind::Integer)
    Int64(size(g,2) + 1) == Int64(col_ind)
end
function nextcolname(g::Gradine)
    Symbol(string("x", size(g,2)+1))
end

# helper functions for insert_single_column!
function _insert_new_single_column!{T}(g::Gradine, v::AbstractVector{T}, col_ind::Symbol)
    push!(index(g), col_ind)
    appendcolumn!(g.schema, col_ind, T)
    Data.streamto!(g, Data.Column, v, 0, size(g,2)+1, g.schema)
end
function _insert_new_single_column!{T}(g::Gradine, v::AbstractVector{T}, col_ind::Integer)
    if isnextcol(g, col_ind)
        name = nextcolname(g)
        push!(index(g), name)
        appendcolumn!(g.schema, string(name), T)
        Data.streamto!(g, Data.Column, v, 0, size(g,2)+1, g.schema)
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind."))
    end
end
function _insert_existing_single_column!{T}(g::Gradine, v::AbstractVector{T},
                                            col_ind::Symbol)
    j = index(g)[col_ind]
    altercolumn!(g.schema, j, string(col_ind), T)
    Data.streamto!(g, Data.Column, v, 0, j, g.schema)
end
function _insert_existing_single_column!{T}(g::Gradine, v::AbstractVector{T},
                                            col_ind::Integer)
    n = names(g)[col_ind]
    altercolumn!(g.schema, j, string(n), T)
    Data.streamto!(g, Data.Column, v, 0, col_ind, g.schema)
end


function insert_single_column!(g::Gradine, v::AbstractVector, col_ind::ColumnIndex)
    if size(g,2) ≠ 0 && size(g,1) ≠ length(v)
        throw(ArgumentError("New columns must have same length as old columns."))
    end
    if haskey(index(g), col_ind)
        _insert_existing_single_column!(g, v, col_ind)
    else
        _insert_new_single_column!(g, v, col_ind)
    end
    v
end


function insert_single_entry!(g::Gradine, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    if haskey(index(g), col_ind)
        j = index(g)[col_ind]
        Data.streamto!(g, Data.Field, v, row_ind, j, g.schema)
    else
        throw(ArgumentError("Cannot assign to non-existent column $col_ind."))
    end
end


function insert_multiple_entries!(g::Gradine, v::AbstractVector,
                                  row_inds::AbstractVector{<:Integer},
                                  col_ind::ColumnIndex)
    if length(v) ≠ length(row_inds)
        throw(ArgumentError("Length of assigned vector did not match length of indices."))
    end
    if haskey(index(g), col_ind)
        j = index(g)[col_ind]
        for i ∈ 1:length(v)
            Data.streamto!(g, Data.Field, v[i], row_inds[i], col_ind, g.schema)
        end
    else
        throw(ArgumentError("Cannot assign to non-existent column $col_ind."))
    end
end


function upgrade_scalar_nonull(g::Gradine, v::Any)
    n = (size(g,2) == 0) ? 1 : size(g,1)
    fill(v, n)
end


# g[SingleColumnIndex] = Vector
function Base.setindex!(g::Gradine, v::AbstractVector, col_ind::ColumnIndex)
    insert_single_column!(g, v, col_ind)
end

# g[SingleColumnIndex] = (single item, expands to nrow(g) if ncol(g) > 0)
function Base.setindex!(g::Gradine, v, col_ind::ColumnIndex)
    insert_single_column!(g, upgrade_scalar_nonull(g, v), col_ind)
end

# TODO continue from dataframes line 396
#=========================================================================================
    </setindex>
=========================================================================================#




#=========================================================================================
    <accessors>
=========================================================================================#
DataTables.head(g::Gradine, nrows::Integer=5) = g[1:nrows, :]
DataTables.tail(g::Gradine, nrows::Integer=5) = g[(end-nrows):end, :]

export head, tail
#=========================================================================================
    </accessors>
=========================================================================================#




