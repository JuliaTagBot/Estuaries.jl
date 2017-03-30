

function hdf5joinpath{N}(args::Union{Vector{String},Tuple{Vararg{String,N}}})
    args = [strip(a, '/') for a ∈ args] 
    join(args, '/')
end

hdf5joinpath(args::String...) = hdf5joinpath(args)

