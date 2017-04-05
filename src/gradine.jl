
const ColumnIndex = Union{Integer, Symbol}


struct Gradine <: AbstractGradine
    grp::GrdGroup
    columns::Vector{AbstractGradineColumn}

    colindex::DataTables.Index

    function Gradine(grp::GrdGroup, cols::Vector, idx::DataTables.Index)
        if !exists(grp, "columns")
            # if we can't do this we are looking at an invalid group
            # TODO throw error if that happens
            g_create(grp, "columns")
        end
        new(grp, cols, idx)
    end
end
export Gradine


#=========================================================================================
    <interface>
=========================================================================================#


DataTables.ncol(g::Gradine) = length(g.columns)
DataTables.nrow(g::Gradine) = (ncol(g) < 1) ? 0 : length(g.columns[1])
Base.size(g::Gradine) = (nrow(g), ncol(g))
Base.size(g::Gradine, idx::Integer) = size(g)[idx]
DataTables.index(g::Gradine) = g.colindex
DataTables.eltypes(g::Gradine) = eltype.(g.columns)
export eltypes
Base.names(g::Gradine) = names(index(g))

columns_group(g::Gradine) = g.grp["columns"]

Base.copy(g::Gradine) = Gradine(copy(g.grp), copy(g.columns), copy(g.colindex))

Base.deepcopy(g::Gradine) =
    Gradine(deepcopy(g.grp), deepcopy(g.columns), deepcopy(g.colindex))

#=========================================================================================
    </interface>
=========================================================================================#


#=========================================================================================
    <getindex>
    TODO make sure these can take nullable vector indices
=========================================================================================#

# g[SingleColumnIndex] ⇒ AbstractGradineColumn
function getindex(g::Gradine, col_ind::Symbol)
    selected_col = index(g)[col_ind]
    g.columns[selected_col]
end

# g[MultiColumnIndex] ⇒ Gradine
function getindex{T<:ColumnIndex}(g::Gradine, col_inds::AbstractVector{T})
    selected_cols = index(g)[col_inds]
    new_cols = g.columns[selected_cols]
    # the below line is a bit different then in DataTables, TODO be sure to test
    Gradine(g.grp, new_cols, DataTables.Index(selected_cols))
end

# g[:] ⇒ Gradine
getindex(g::Gradine, colon::Colon) = copy(g)

# g[SingleRowIndex, SingleColumnIndex] ⇒ Scalar
function getindex(g::Gradine, row_ind::Integer, col_ind::ColumnIndex)
    selected_col = index(g)[col_ind]
    g.columns[selected_col][row_ind]
end

# SingleRowIndex, MultiColumnIndex ⇒ DataTable
# Note that this MUST return a DataTable rather then a Gradine, since you can't load
# subsets of arrays as JldDataset objects!
function getindex{T<:ColumnIndex}(g::Gradine, row_ind::Integer, col_inds::AbstractVector{T})
    selected_cols = index(g)[col_inds]
    new_cols = Any[c[[row_ind]] for c ∈ g.columns[selected_cols]]
    DataTable(new_cols, DataTables.Index(names(g)[selected_cols]))
end

# g[MultiRowIndex, SingleColumnIndex] ⇒ AbstractVector
function getindex{T<:Integer}(g::Gradine, row_inds::AbstractVector{T}, col_ind::ColumnIndex)
    selected_col = index(g)[col_ind]
    g.columns[selected_col][row_inds]
end

# MultiRowIndex, MultiColumnIndex ⇒ DataTable
function getindex{R<:Integer, T<:ColumnIndex}(g::Gradine,
                                              row_inds::AbstractVector{R},
                                              col_inds::AbstractVector{T})
    selected_cols = index(g)[col_inds]
    new_cols = Any[c[row_inds] for c ∈ g.columns[selected_cols]]
    DataTable(new_cols, DataTables.Index(names(g)[selected_cols]))
end

# g[:, SingleColumnIndex] ⇒ Vector
# g[:, MultiColumnIndex] ⇒ Gradine
getindex(g::Gradine, ::Colon, col_ind::ColumnIndex) = g[col_ind]
getindex{T<:ColumnIndex}(g::Gradine, ::Colon, col_inds::AbstractVector{T}) = g[col_inds]

# g[SingleRowIndex, :] ⇒ DataTable
getindex(g::Gradine, row_ind::Integer, col_inds::Colon) = g[[row_ind], col_inds]

# g[MultiRowIndex, :] ⇒ DataTable
function getindex{R<:Integer}(g::Gradine, row_inds::AbstractVector{R}, col_inds::Colon)
    new_cols = Any[c[row_inds] for c ∈ g.columns]
    DataTable(new_cols, copy(index(g)))
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
    ncol(g) + 1 == Int64(col_ind)
end

# we use the same default column names as DataTables
function nextcolname(g::Gradine)
    Symbol(string("x", ncol(g) + 1))
end


# helper function for insert_single_column
function _insert_new_single_column!(g::Gradine, c::AbstractGradineColumn,
                                    col_ind::Symbol)
    push!(index(g), col_ind)
    push!(g.columns, c)
end

# helper function for insert_single_column
function _insert_new_single_column!(g::Gradine, c::AbstractGradineColumn,
                                    col_ind::Integer)
    if isnextcol(g, col_ind)
        push!(index(g), nextcolname(g))
        push!(g.columns, c)
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind."))
    end
end

# this will respect whether we give a NullableVector
function insert_single_column!(g::Gradine, v::AbstractVector, col_ind::ColumnIndex)
    if ncol(g) ≠ 0 && nrow(g) ≠ length(v)
        throw(ArgumentError("New columns must have same length as old columns."))
    end
    c = gradinecolumn!(columns_group(g), col_ind, v)
    if haskey(index(g), col_ind)
        j = index(g)[col_ind]
        g.columns[j] = c
    else
        _insert_new_single_column!(g::Gradine, c, col_ind)
    end
    c
end

function insert_single_entry!(g::Gradine, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    if haskey(index(g), col_ind)
        # will throw an error if file is read only
        g.columns[index(g)[col_ind]][row_ind] = v
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind."))
    end
    v
end

function insert_multiple_entries!(g::Gradine, v::Any,
                                  row_inds::AbstractVector{<:Integer},
                                  col_ind::ColumnIndex)
    if haskey(index(g), col_ind)
        g.columns[index(g)[col_ind]][row_inds] = v
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind."))
    end
    v
end

function upgrade_scalar_nonull(g::Gradine, v::Any)
    n = (ncol(g) == 0) ? 1 : nrow(g)
    fill(v, n)
end

# g[SingleColumnIndex] = Vector
function setindex!(g::Gradine, v::AbstractVector, col_ind::ColumnIndex)
    insert_single_column!(g, v, col_ind)
end

# expand from single item
function setindex!(g::Gradine, v, col_ind::ColumnIndex)
    insert_single_column!(g, upgrade_scalar_nonull(g, v), col_ind)
end

# g[MultiColumnIndex] = DataTable
function setindex!{T<:ColumnIndex}(g::Gradine, data::DataTable, col_inds::AbstractVector{T})
    if prod(completecases(data))
        for i ∈ 1:length(col_inds)
            insert_single_column!(g, convert(Vector, data[i]), col_inds[j])
        end
    else
        error("Null data element support for Gradines not implemented yet!")
    end
end
function setindex!(g::Gradine, data::DataTable,
                   col_inds::Union{BitArray,AbstractVector{Bool}})
    setindex!(g, data, find(col_inds))
end

# g[MultiColumnIndex] = AbstractVector (repeated for each column)
function setindex!{T<:ColumnIndex}(g::Gradine, v::AbstractVector, col_inds::AbstractVector{T})
    for col_ind ∈ col_inds
        setindex!(g, v, col_ind)
    end
end
function setindex!(g::Gradine, v::AbstractVector,
                   col_inds::Union{BitArray,AbstractVector{Bool}})
    setindex!(g, v, find(col_inds))
end


# g[MultiColumnIndex] = Single Item
# (repated for each column; expands to nrow(dt) if ncol(dt)>0)
function setindex!{T<:ColumnIndex}(g::Gradine, val::Any, col_inds::AbstractVector{T})
    for col_ind ∈ col_inds
        setindex!(g, val, col_ind)
    end
    g
end
function setindex!(g::Gradine, val::Any, col_inds::AbstractVector{Bool})
    setindex!(g, val, find(col_inds))
end

# g[:] = AbstractVector or single item
setindex!(g::Gradine, v, ::Colon) = (g[1:size(g, 2)] = v; g)

# g[SingleRowIndex, SingleColumnIndex] = single item
function setindex!(g::Gradine, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    insert_single_entry!(g, v, row_ind, col_ind)
end
function setindex!{T<:ColumnIndex}(g::Gradine, v::Any, row_ind::Integer,
                                   col_inds::AbstractVector{T})
    for col_ind ∈ col_inds
        insert_single_entry!(g, v, row_ind, col_ind)
    end
    g
end

# g[SingleRowIndex, MultiColumnIndex] = 1-Row DataTable
# doesn't check that this is indeed a 1-row table
function setindex!(g::Gradine, v::DataTable, row_ind::Integer,
                   col_inds::AbstractVector{<:ColumnIndex})
    for j ∈ 1:length(col_inds)
        insert_single_entry!(g, v[j][1], row_ind, col_inds[j])
    end
end
function setindex!(g::Gradine, v::DataTable, row_ind::Integer,
                   col_inds::AbstractVector{Bool})
    setindex!(g, v, row_ind, find(col_inds))
end

# g[MultiRowIndex, SingleColumnIndex] = AbstractVector
function setindex!(g::Gradine, v::AbstractVector, row_inds::AbstractVector{<:Integer},
                   col_ind::ColumnIndex)
    insert_multiple_entries!(g, v, row_inds, col_ind)
    g
end
function setindex!(g::Gradine, v::AbstractVector, row_inds::AbstractVector{Bool},
                   col_ind::ColumnIndex)
    setindex!(g, v, find(row_inds), col_ind)
end

# g[MultiRowIndex, SingleColumnIndex] = single item
function setindex!(g::Gradine, v::Any, row_inds::AbstractVector{<:Integer},
                   col_ind::ColumnIndex)
    insert_multiple_entries!(g, v, row_inds, col_ind)
    g
end
function setindex!(g::Gradine, v::Any, row_inds::AbstractVector{Bool},
                   col_ind::ColumnIndex)
    setindex!(g, v, find(row_inds), col_ind)
end

# g[MultiRowIndex, MultiColumnIndex] = DataTable
function setindex!(g::Gradine, v::DataTable, row_inds::AbstractVector{<:Integer},
                   col_inds::AbstractVector{<:ColumnIndex})
    for j ∈ 1:length(col_inds)
        insert_multiple_entries!(g, v[:, j], row_inds, col_inds[j])
    end
    g
end
function setindex!(g::Gradine, v::DataTable, row_inds::AbstractVector{<:Integer},
                   col_inds::AbstractVector{Bool})
    setindex!(g, v, row_inds, find(col_inds))
end
function setindex!(g::Gradine, v::DataTable, row_inds::AbstractVector{Bool},
                   col_inds::AbstractVector{<:ColumnIndex})
    setindex!(g, v, find(row_inds), col_inds)
end
function setindex!(g::Gradine, v::DataTable, row_inds::AbstractVector{Bool},
                   col_inds::AbstractVector{Bool})
    setindex!(g, v, find(row_inds), find(col_inds))
end

# g[MultiRowIndex, MultiColumnIndex] = AbstractVector
function setindex!(g::Gradine, v::AbstractVector, row_inds::AbstractVector{<:Integer},
                   col_inds::AbstractVector{<:ColumnIndex})
    for col_ind ∈ col_inds
        insert_multiple_entries!(g, v, row_inds, col_ind)
    end
    g
end
function setindex!(g::Gradine, v::AbstractVector, row_inds::AbstractVector{<:Integer},
                   col_inds::AbstractVector{Bool})
    setindex!(g, v, row_inds, find(col_inds))
end
function setindex!(g::Gradine, v::AbstractVector, row_inds::AbstractVector{Bool},
                   col_inds::AbstractVector{<:ColumnIndex})
    setindex!(g, v, find(row_inds), col_inds)
end
function setindex!(g::Gradine, v::AbstractVector, row_inds::AbstractVector{Bool},
                   col_inds::AbstractVector{Bool})
    setindex!(g, v, find(row_inds), find(col_inds))
end

# g[MultiRowIndex, MultiColumnIndex] = single item
function setindex!(g::Gradine, v::Any, row_inds::AbstractVector{<:Integer},
                   col_inds::AbstractVector{<:ColumnIndex})
    for col_ind ∈ col_inds
        insert_multiple_entries!(g, v, row_inds, col_ind)
    end
    g
end
function setindex!(g::Gradine, v::Any, row_inds::AbstractVector{<:Integer},
                   col_inds::AbstractVector{Bool})
    setindex!(g, v, row_inds, find(col_inds))
end
function setindex!(g::Gradine, v::Any, row_inds::AbstractVector{Bool},
                   col_inds::AbstractVector{<:ColumnIndex})
    setindex!(g, v, find(row_inds), col_inds)
end
function setindex!(g::Gradine, v::Any, row_inds::AbstractVector{Bool},
                   col_inds::AbstractVector{Bool})
    setindex!(g, v, find(row_inds), find(col_inds))
end

# g[:] = DataTable; g[:, :] = DataTable
function setindex!(g::Gradine, v::DataTable, row_inds::Colon, col_inds::Colon=Colon())
    empty!(g)
    for name ∈ names(v)
        insert_single_column!(g, v[name], name)
    end
    g
end

# g[:, :] = ...
setindex!(g::Gradine, v, ::Colon, ::Colon) = (g[1:size(g,1), 1:size(g,2)] = v; g)

# g[Any, :] = ...
setindex!(g::Gradine, v, row_inds, ::Colon) = (g[row_inds, 1:size(g,2)] = v; g)

# special deletion assignment
setindex!(g::Gradine, x::Void, col_ind::Integer) = delete!(g, col_ind)
#=========================================================================================
    </setindex>
=========================================================================================#


#=========================================================================================
    <Mutating Associative Methods>
=========================================================================================#
function Base.empty!(g::Gradine)
    delete!.(g.columns)
    empty!(g.columns)
    empty!(g.colindex)
    g
end

function Base.merge!(g::DataTable, others::Union{AbstractDataTable, Gradine}...)
    for other ∈ others
        for n ∈ names(other)
            g[n] = other[n]
        end
    end
    g
end
#=========================================================================================
    </Mutating Associative Methods>
=========================================================================================#


#=========================================================================================
    <deletion, subsetting>
=========================================================================================#
function Base.delete!(g::Gradine, inds::Vector{<:Integer})
    for ind ∈ sort(inds, rev=true)
        if 1 ≤ ind ≤ ncol(g)
            delete!(g.columns[ind])
            splice!(g.columns, ind)
            delete!(index(g), ind)
        else
            throw(ArgumentError("Can't delete a non-existent Gradine column."))
        end
    end
    g
end
Base.delete!(g::Gradine, c::Integer) = delete!(g, [c])
Base.delete!(g::Gradine, c::Any) = delete!(g, index(g)[c])

# we can't support deleting rows
#=========================================================================================
    </deletion, subsetting>
=========================================================================================#


# TODO continue implementing DataTables methods from datatable.jl line 749


#=========================================================================================
    <constructors>
=========================================================================================#
function Gradine(grp::GrdGroup)
    if "columns" ∈ names(grp)
        colnames = names(grp["columns"])
        cols = [gradinecolumn(grp["columns"][n]) for n ∈ colnames]
        return Gradine(grp, cols, DataTables.Index(Symbol.(colnames)))
    else
        return Gradine(grp, AbstractGradineColumn[], DataTables.Index())
    end
end

Gradine(datafile::GrdFile, group_name::String) = Gradine(datafile[group_name])
Gradine(datafile::GrdFile) = Gradine(datafile, "/")

# TODO update these to accomodate reading
function Gradine(filename::String, mode::String, group_name::String)
    datafile = h5open(filename, mode)
    Gradine(datafile, group_name)
end
function Gradine(filename::String, mode::String)
    datafile = h5open(filename, mode)
    Gradine(datafile)
end
function Gradine(filename::String; group_name::String="/", mode::String="r")
    datafile = if isfile(filename)
        if ishdf5(filename)
            jldopen(filename, mode)
        else
            throw(ArgumentError("File $filename is not an HDF5 (JLD) file."))
        end
    else
        jldopen(filename, "w")
    end
    Gradine(datafile, group_name)
end

# construction of an all-null Gradine
function Gradine(grp::GrdGroup, colnames::Vector{Symbol}, coltypes::Vector{DataType},
                 nrows::Integer)
    if "columns" ∈ HDF5.names(grp)
        throw(ArgumentError("Must create new Gradine in HDF5 group without columns."))
    end
    if length(colnames) ≠ length(coltypes)
        throw(ArgumentError("Must supply same number of column names and types."))
    end
    cols = Vector{AbstractGradineColumn}(length(colnames))
    for i ∈ 1:length(colnames)
        cols[i] = nullgradinecolumn!(grp[hdf5joinpath("columns", string(colnames[i]))],
                                     coltypes[i], nrows)  # TODO check on this!!!
    end
    Gradine(grp, cols, DataTables.Index(colnames))
end
function Gradine(grdfile::GrdFile, group_name::String, colnames::Vector{Symbol},
                 coltypes::Vector{DataType}, nrows::Integer)
    Gradine(grdfile[group_name], colnames, coltypes, nrows)
end
function Gradine(grdfile::GrdFile, colnames::Vector{Symbol}, coltypes::Vector{DataType},
                 nrows::Integer)
    Gradine(grdfile, "/", colnames, coltypes, nrows)
end

# construct an all-null gradine using keyword arguments to name columns and give types
function Gradine(grp::GrdGroup, nrows::Integer; kwargs::DataType...)
    colnames = [k[1] for k ∈ kwargs]
    coltypes = [k[2] for k ∈ kwargs]
    Gradine(grp, colnames, coltypes, nrows)
end
function Gradine(grdfile::GrdFile, group_name::String, nrows::Integer; kwargs::DataType...)
    Gradine(grdfile[group_name], nrows; kwargs...)
end
function Gradine(grdfile::GrdFile, nrows::Integer; kwargs::DataType...)
    Gradine(grdfile, "/", nrows; kwargs...)
end

# construct a Gradine from a supplied list of columns
function Gradine(grp::GrdGroup; kwargs::AbstractVector...)
    if "columns" ∈ HDF5.names(grp)
        throw(ArgumentError("Must create new Gradine in HDF5 group without columns."))
    end
    colnames = [k[1] for k ∈ kwargs]
    cols_ = [k[2] for k ∈ kwargs]
    cols = Vector{AbstractGradineColumn}(length(colnames))
    for i ∈ 1:length(colnames)
        cols[i] = gradinecolumn!(grp["columns"], colnames[i], cols_[i])
    end
    Gradine(grp, cols, DataTables.Index(colnames))
end
#=========================================================================================
    </constructors>
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




