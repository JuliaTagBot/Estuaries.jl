
const ColumnIndex = Union{Integer, Symbol}


type Gradine <: AbstractGradine
    grp::HDF5Group
    columns::Vector{AbstractGradineColumn}

    # at least for now, this is not saved to file, it will have to be inferred from what is
    colindex::DataTables.Index

    function Gradine(grp::HDF5Group, cols::Vector, idx::DataTables.Index)
        if !exists(grp, "columns")
            # if we can't do this we are looking at an invalid group
            # TODO throw error if that happens
            g_create(grp, "columns")
        end
        new(grp, cols, idx)
    end
    Gradine(grp::HDF5Group) = Gradine(grp, AbstractGradineColumn[], DataTables.Index())
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
export DataTables.eltypes
Base.names(g::Gradine) = names(index(g))

columns_group(g::Gradine) = g.grp["columns"]

Base.copy(g::Gradine) = Gradine(copy(g.grp), copy(g.columns), copy(g.colindex))


#=========================================================================================
    </interface>
=========================================================================================#


#=========================================================================================
    <getindex>
    TODO make sure these can take nullable vector indices
=========================================================================================#

# SingleColumnIndex ⇒ AbstractGradineColumn
function getindex(g::Gradine, col_ind::Symbol)
    selected_col = index(g)[col_ind]
    g.columns[selected_col]
end

# MultiColumnIndex ⇒ Gradine
function getindex{T<:ColumnIndex}(g::Gradine, col_inds::AbstractVector{T})
    selected_cols = index(g)[col_inds]
    new_cols = g.columns[selected_cols]
    # the below line is a bit different then in DataTables, TODO be sure to test
    Gradine(g.grp, new_cols, DataTables.Index(selected_cols))
end

# : => Gradine
getindex(g::Gradine, colon::Colon) = copy(g)

# SingleRowIndex, SingleColumnIndex ⇒ Scalar
function getindex(g::Gradine, row_ind::Integer, col_ind::ColumnIndex)
    selected_col = index(g)[col_ind]
    g.columns[selected_col][row_ind]
end

# SingleRowIndex, MultiColumnIndex ⇒ DataTable
# Note that this MUST return a DataTable rather then a Gradine, since you can't load
# subsets of arrays as HDF5Dataset objects!
function getindex{T<:ColumnIndex}(g::Gradine, row_ind::Integer, col_inds::AbstractVector{T})
    selected_cols = index(g)[col_inds]
    new_cols = Any[c[[row_ind]] for c ∈ g.columns[selected_cols]]
    DataTable(new_cols, DataTables.Index(names(g)[selected_cols]))
end

# MultiRowIndex, SingleColumnIndex ⇒ AbstractVector
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

# :, SingleColumnIndex ⇒ Vector
# :, MultiColumnIndex ⇒ Gradine
getindex(g::Gradine, ::Colon, col_ind::ColumnIndex) = g[col_ind]
getindex{T<:ColumnIndex}(g::Gradine, ::Colon, col_inds::AbstractVector{T}) = g[col_inds]

# SingleRowIndex, : ⇒ DataTable
getindex(g::Gradine, row_ind::Integer, col_inds::Colon) = g[[row_ind], col_inds]

# MultiRowIndex, : ⇒ DataTable
function getindex{R<:Integer}(g::Gradine, row_inds::AbstractVector{R}, col_inds::Colon)
    new_cols = Any[c[row_inds] for c ∈ g.columns]
    DataTable(new_cols, copy(index(g)))
end

# :, : ⇒ Gradine
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

# TODO for now we only accept regular vectors
function insert_single_column!(g::Gradine, v::Vector, col_ind::ColumnIndex)
    if ncol(g) ≠ 0 && nrow(g) ≠ length(v)
        throw(ArgumentError("New columns must have same length as old columns."))
    end
    c = GradineColumn!(columns_group(g), col_ind, v)
    if haskey(index(g), col_ind)
        j = index(g)[col_ind]
        g.columns[j] = c
    else
        _insert_new_single_column!(g::Gradine, c, col_ind)
    end
    c
end

function insert_single_entry!(g::Gradine, v::Any, row_ind::Real, col_ind::ColumnIndex)
    if haskey(index(g), col_ind)
        # will throw an error if file is read only
        g.columns[index(g)[col_ind]][row_ind] = v
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind."))
    end
    v
end

function insert_multiple_entries!{T<:Integer}(g::Gradine, v::Any,
                                              row_inds::Vector{T},
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

# SingleColumnIndex ⇒ Vector
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


# TODO continue implementing DataTables methods from line 427

#=========================================================================================
    </setindex>
=========================================================================================#


#=========================================================================================
    <constructors>
=========================================================================================#
Gradine(datafile::HDF5File, group_name::String) = Gradine(datafile[group_name])
Gradine(datafile::HDF5File) = Gradine(datafile, "/")

function Gradine(filename::String, mode::String, group_name::String)
    datafile = h5open(filename, mode)
    Gradine(datafile, group_name)
end
function Gradine(filename::String, mode::String)
    datafile = h5open(filename, mode)
    Gradine(datafile)
end
function Gradine(filename::String; group_name::String="/", mode::String="r")
    datafile = if isfile(filename) && ishdf5(filename)
        h5open(filename, mode)
    else
        h5open(filename, "w")
    end
    Gradine(datafile, group_name)
end
#=========================================================================================
    </constructors>
=========================================================================================#


