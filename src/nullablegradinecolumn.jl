
struct NullableGradineColumn{T} <: AbstractGradineColumn{Nullable{T}}
    values::GrdDataset
    isnull::GrdDataset

    # note that isnull will be a stored Vector rather than a BitArray
    # this is consistent with the convention of NullableArrays

    function NullableGradineColumn{T}(values::GrdDataset, isnull::GrdDataset) where T
        new(values, isnull)
    end
end


#=========================================================================================
    <interface>
=========================================================================================#
# AbstractGradineColumn methods are found in gradinecolumn.jl

Base.isnull(gc::NullableGradineColumn, idx::Integer) = gc.isnull[idx][1]
Base.isnull(gc::NullableGradineColumn, idx::AbstractVector{<:Integer}) = gc.isnull[idx][1]
Base.eltype{T}(gc::NullableGradineColumn{T}) = Nullable{T}

Base.delete!(gc::NullableGradineColumn) = (delete!(gc.values); delete!(gc.isnull))

#=========================================================================================
    </interface>
=========================================================================================#


#=========================================================================================
    <constructors>
=========================================================================================#
function NullableGradineColumn(values::GrdDataset, isnull::GrdDataset)
    NullableGradineColumn{eltype(values)}(values, isnull)
end

function NullableGradineColumn!{T}(grp::GrdGroup, name::Symbol,
                                   values::AbstractVector{T},
                                   isnull::AbstractVector{Bool})
    path_ = path_values(grp, name)
    path_isnull_ = path_isnull(grp, name)
    grp[path_] = values
    grp[path_isnull_] = convert(Vector{Bool}, isnull)
    NullableGradineColumn{T}(grp[path_], grp[path_isnull_])
end
function NullableGradineColumn!{T}(grp::GrdGroup, name::Symbol, v::AbstractVector{T})
    NullableGradineColumn!(grp, name, v, zeros(Bool, length(v)))
end
function NullableGradineColumn!{T}(grp::GrdGroup, name::Symbol, v::NullableVector{T})
    NullableGradineColumn!(grp, name, v.values, v.isnull)
end

function NullableGradineColumn!{T}(grdfile::GrdFile, group_name::String, name::Symbol,
                                   values::AbstractVector{T}, isnull::AbstractVector{Bool})
    NullableGradineColumn!(grdfile[group_name], name, values, isnull)
end
function NullableGradineColumn!{T}(grdfile::GrdFile, group_name::String,
                                   values::AbstractVector{T})
    NullableGradineColumn!(grdfile[group_name], name, values)
end
function NullableGradineColumn!{T}(grdfile::GrdFile, group_name::String,
                                   v::NullableVector{T})
    NullableGradineColumn!(grdfile[group_name], name, v)
end

# creates a column of all nulls
function nullgradinecolumn!{T}(grp::GrdGroup, ::Type{T}, nrows::Integer)
    NullableGradineColumn{T}(Vector{T}(nrows), zeros(Bool, nrows))
end
#=========================================================================================
    </constructors>
=========================================================================================#


#=========================================================================================
    <getindex>
=========================================================================================#
function Base.getindex{T}(gc::NullableGradineColumn{T}, idx::Integer)::Nullable{T}
    if isnull(gc, idx)
        return Nullable{T}()
    end
    Nullable{T}(gc.values[idx][1])
end

function Base.getindex{T}(gc::NullableGradineColumn{T}, idx::Colon)::NullableVector{T}
    NullableArray{T,1}(gc.values[idx], gc.isnull[idx])
end

# HDF5 datasets don't support vector indices
function getindex{T}(gc::NullableGradineColumn{T}, idx::AbstractVector{<:Integer}
                     )::NullableVector{T}
    NullableArray{T,1}([gc.values[i][1] for i ∈ idx], [gc.isnull[i][1] for i ∈ idx])
end

function getindex{T}(gc::NullableGradineColumn{T}, idx::AbstractVector{Bool}
                     )::NullableVector{T}
    getindex(gc, find(idx))
end
#=========================================================================================
    </getindex>
=========================================================================================#


#=========================================================================================
    <setindex>
=========================================================================================#
function setindex!(gc::NullableGradineColumn, v::Nullable, idx)
    if isnull(v)
        gc.isnull[idx] = true
    else
        gc.isnull[idx] = false
        gc.values[idx] = get(v) end v
end

function setindex!(gc::NullableGradineColumn, v::Any, idx)
    gc.isnull[idx] = false
    gc.values[idx] = v
    v
end
#=========================================================================================
    </setindex>
=========================================================================================#




