
# TODO working on regular column first, then I'll work on this version
# will probably go into a separate file
struct NullableGradineColumn{T} <: AbstractGradineColumn{T}
    values::HDF5Dataset
    isnull::HDF5Dataset

    function NullableGradineColumn{T}(values::HDF5Dataset, isnull::HDF5Dataset) where T
        new(values, isnull)
    end
end

struct GradineColumn{T} <: AbstractGradineColumn{T}
    values::HDF5Dataset
    eltype::Type{T}

    GradineColumn{T}(values::HDF5Dataset) where T = new(values, T)
end
export GradineColumn


#=========================================================================================
    <interface>
=========================================================================================#
Base.length(gc::AbstractGradineColumn) = length(gc.values)
Base.size(gc::AbstractGradineColumn) = size(gc.values)
Base.eltype{T}(gc::AbstractGradineColumn{T}) = T

function name(gc::AbstractGradineColumn)
    Symbol(string(split(name(parent(gc.values))), '/')[end])
end

getindex(gc::GradineColumn, idx) = getindex(gc.values, idx)
#=========================================================================================
    </interface>
=========================================================================================#


#=========================================================================================
    <constructors>
=========================================================================================#
GradineColumn(values::HDF5Dataset) = GradineColumn{eltype(values)}(values)

# note this gets a ! because it modifies grp
function GradineColumn!(grp::HDF5Group, name::Symbol, values::Vector)
    path = hdf5joinpath(string(name), "values")
    grp[path] = values
    GradineColumn(grp[path])
end

function GradineColumn!(hdf5file::HDF5File, group_name::String, name::Symbol, values::Vector)
    GradineColumn!(hdf5file[group_name])
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
function getindex{T<:Integer}(gc::GradineColumn, idx::Vector{T})
    [gc.values[i][1] for i ∈ idx]
end

#=========================================================================================
    </getindex>
=========================================================================================#






