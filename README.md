# Estuaries

This package is designed to provide a convenient and much expanded interface for any Julia objects implementing the
[DataStreams.jl](https://github.com/JuliaData/DataStreams.jl) interface for tabular data.  The idea is that the `DataStreams` interface only requires the bare
minimum of methods to be implemented for the handling of tabular data, while Estuaries provides greatly expanded functionality without requiring the user to
define any more methods than are specified by DataStreams.  An `Estuary` object has a fully-implemented DataFrames interface, so a user can enable all the
functionality of DataFrames (where applicable) to any object `src` with a DataStreams interface simply by doing `Estuary(src)`.  This can work for an object
which is a `Data.Source`, `Data.Sink` or both.

## Usage Example
This package provides access to the `Estuary` type which can wrap any tabular data source or sink.  For example, the
[Feather.jl](https://github.com/JuliaStats/Feather.jl) package implements both the Apache [arrow](https://github.com/apache/arrow) on-disk format "feather" and
the `DataStreams` interface.  The following code opens a feather file and wraps it in an `Estuary`.



