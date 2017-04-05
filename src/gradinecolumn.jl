

struct GradineColumn{T} <: AbstractGradineColumn{T}
    values::GrdDataset

    GradineColumn{T}(values::GrdDataset) where T = new(values)
end
export GradineColumn


#=========================================================================================
    <interface>
=========================================================================================#
Base.length(gc::AbstractGradineColumn) = length(gc.values)
Base.size(gc::AbstractGradineColumn) = size(gc.values)
Base.eltype{T}(gc::AbstractGradineColumn{T}) = T

# parent is assumed to be same for all datasets for other types
parent(gc::AbstractGradineColumn) = parent(gc.values)

function name(gc::AbstractGradineColumn)
    Symbol(string(split(name(parent(gc))), '/')[end])
end

getindex(gc::GradineColumn, idx) = getindex(gc.values, idx)

Base.delete!(gc::GradineColumn) = delete!(gc.values)
#=========================================================================================
    </interface>
=========================================================================================#


#=========================================================================================
    <constructors>
=========================================================================================#
GradineColumn(values::GrdDataset) = GradineColumn{eltype(values)}(values)

# note this gets a ! because it modifies grp
function GradineColumn!{T}(grp::GrdGroup, name::Symbol, values::AbstractVector{T})
    path = path_values(grp, name)
    grp[path] = values
    GradineColumn{T}(grp[path])
end

function GradineColumn!{T}(grdfile::GrdFile, group_name::String, name::Symbol,
                           values::AbstractVector{T})
    GradineColumn!(grdfile[group_name], name, values)
end

#=========================================================================================
    </constructors>
=========================================================================================#


#=========================================================================================
    <getindex>
=========================================================================================#
function getindex{T<:Integer}(gc::GradineColumn, idx::Union{Colon, T, UnitRange{T}})
    gc.values[idx]
end
# HDF5 datasets don't support vector indices
function getindex{T<:Integer}(gc::GradineColumn, idx::AbstractVector{T})
    [gc.values[i][1] for i ∈ idx]
end

function getindex(gc::GradineColumn, idx::AbstractVector{Bool})
    getindex(gc, find(idx))
end

#=========================================================================================
    </getindex>
=========================================================================================#


#=========================================================================================
    <setindex>
=========================================================================================#
function setindex!(gc::GradineColumn, v, idx)
    gc.values[idx] = v
end
#=========================================================================================
    </setindex>
=========================================================================================#



#=========================================================================================
    <general constructors>

    These are constructors which determine the column type on their own.
=========================================================================================#
function gradinecolumn!(grp::GrdGroup, name::Symbol, v::AbstractVector)
    GradineColumn!(grp, name, v)
end

function gradinecolumn!(grp::GrdGroup, name::Symbol, v::NullableVector)
    NullableGradineColumn!(grp, name, v)
end

function gradinecolumn!(grdfile::GrdFile, group_name::String, name::Symbol, v::AbstractVector)
    GradineColumn!(grdfile, group_name, name, v)
end

function gradinecolumn!(grdfile::GrdFile, group_name::String, name::Symbol, v::NullableVector)
    NullableGradineColumn!(grdfile, group_name, name, v)
end

# for loading from file
function gradinecolumn(grp::GrdGroup)
    colname = group_name(grp)
    leaves = Set(names(grp))

    # this is surely not the most efficient way of doing this
    if Set(["values"]) == leaves
        dset = grp["values"]
        return GradineColumn{eltype(dset)}(dset) 
    elseif Set(["values", "isnull"]) == leaves
        dset = grp["values"]
        dset_isnull = grp["isnull"]
        return NullableGradineColumn{eltype(dset)}(dset, dset_isnull)
    else
        throw(ArgumentError("Attempted to construct column from invalid HDF5 group."))
    end
end

export gradinecolumn, gradinecolumn!
#=========================================================================================
    </general constructors>
=========================================================================================#



