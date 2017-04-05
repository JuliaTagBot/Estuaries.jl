

function hdf5joinpath{N}(args::Union{Vector{String},Tuple{Vararg{String,N}}})
    args = [strip(a, '/') for a ∈ args] 
    join(args, '/')
end

hdf5joinpath(args::String...) = hdf5joinpath(args)

group_name(grp::GrdGroup) = convert(String, split(HDF5.name(grp), '/')[end])

# these are the standard paths for GradineColumns
path_values(grp::GrdGroup, name::Symbol) = hdf5joinpath(string(name), "values")

path_isnull(grp::GrdGroup, name::Symbol) = hdf5joinpath(string(name), "isnull")

