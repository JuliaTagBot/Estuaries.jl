
abstract type AbstractGradine end
abstract type AbstractGradineColumn{T} <: AbstractVector{T} end

# this is where we define filetypes and group types
const GrdFile = JLD.JldFile
const GrdGroup = JLD.JldGroup
const GrdDataset = JLD.JldDataset


