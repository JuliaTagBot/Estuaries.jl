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
```julia
s = Feather.Source(filename)
E = Estuaries.Source(s)

x = E[1, 1]  # gets the first element of the first column
v1 = E[1]  # gets the first column as the appropriate vector type
y = E[2, :colname]  # columns can also be accessed through their names

data = E[1:100, 1:4]  # blocks of data will be returned as DataTables
```
Note that `Estuary` objects behave very much like `DataTable`s.  When in doubt, use an `Estuary` just as you would a `DataTabe`, it is intended to have all and
only the indexing features of `DataTable`s.

If you have an object `sourcesink` that implements both the `DataStreams` source and sink interfaces, you can do
```julia
E = Estuary(sourcesink)

E[1, 1]  # all the indexing methods work as before

E[1, 1] = gamma(im)  # except that now you can also assign elements
```
Note that you can also do `Estuary(source, sink)` where `source` and `sink` are separate objects that implement the `DataStreams` source and sink interfaces
respectively.  It is expected that a single instance of `Estuary` represents a single object, so you should only do this if `source` and `sink` both point to
the same object.


