# Proposed Overhaul of the `DataStreams` Interface

At the risk of getting to broad, let me try to describe the problems that I think `DataStreams` is trying to solve (of course, this is just my opinion and may
not be settled on).  There are $\ge 3$ major problems which need to be addressed when performing data manipulation:

1. **Transformation and Integration:** This includes joining, grouping, mapping and is the only one of these three problems which is usually addressed.  It is
   (more or less) solved by the various implementations of dataframes and databases.  It is intended that this sort of stuff is pretty much done by the time we
   get to `DataStreams` (though it already allows for simple 1 to 1 mappings).
2. **The API Problem:** Different data sources have a bewildering variety of different API's.  Having a uniform interface for all of them is really hard.
3. **The ABI Problem:** You would think the problem of binary formats would be solved by having API's, but this isn't quite true.  The reason for this is that
   the optimal way of calling said API's depends on the binary format.  (This is largely just the distinction between row-wise and column-wise data.)

I think `DataStreams` should attempt to solve 2 and 3 by providing a way to create a uniform interface for any type of data source/sink by writing a bare
minimum ($\ll 100$ lines of code in most cases) interface.  It should take as little time and effort as possible to implement `DataStreams` on a new tabular
data source/sink, I believe that was the original goal.  I think the special properties of Julia (speed, multiple-dispatch/overloading, metaprogramming) present
us with a unique opportunity to do this well.

I'll list only things that I think should be *changed* rather than describing the whole existing API.


## Overhaul of `Data.streamfrom`
At least one of the following methods should be defined for every source:

- `Data.streamfrom(src, row, col)`.  Typing is handled by the schemas, see below.
- `Data.streamfrom(src, row::AbstractVector, col)`.  This allows column, or partial column based streaming.
- `Data.streamfrom(src, col)`.  This allows column based streaming, but only for the whole column.

I propose that we eliminate `Data.Column` and `Data.Field` entirely, and instead use Julia's built-in introspection functions to determine which of the above
methods is defined for the source (`Data.StreamType` can be used for something else, see below).  This cuts down on how much code needs to be written by the
user.  For the time being `row` and `col` should be `Integer`s (or, in the case of `row`, `AbstractVector{<:Integer}`).  Eventually we should try to address
non-integer indexing schemes, so we won't assert this, but let's put that on the back-burner.

Note that the user can't define `Data.streamfrom(src, row, col::AbstractVector)`.  This is because there'd be little advantage to doing this because of Julia's
in-memory data format.  Eventually we could allow this to be defined possibly using `NamedTuples`, but it doesn't seem like a priority.

### Typing
Instead of explicitly passing type parameters to the above, the appropriate types will be inferred based on the schema.  For the case of columns, by default it
is assumed that a `Vector{T}` is returned unless the element-type is `Nullable{T}`, in which case it is assumed to be `NullableVector{T}`.  After 1.0, the types
in the schema should be `Union` types if there are nulls, and we can always assume `Vector{T}`.

To override these assumptions, we allow the user to optionally define:

- `Data.vectortype(src, ::Type{T})`.  This would return the type to be returned for column with element type `T`.
- `Data.vectortype(src, col)`.  This allows users to specify special vector types for specific columns. It needn't be defined, but if it is, it overrides the
    above.

### Repurposed `Data.StreamType`
The source should define

- `Data.streamtype(src)`. This would return either `Data.Column` or `Data.Row`, defaulting to `Data.Column`.  See below.

If `Data.Column`, streaming occurs one column at a time using `Data.streamfrom(src, col)` or `Data.streamfrom(src, row::AbstractVector, col)` if available.  If
`Data.Row`, streaming occurs one row at a time using `Data.streamfrom(src, row, col)`.  The reason for this is that some data sources (i.e. some SQL) are
serialized in rows (I think, regardless they are easier to access by row, whether this is an artifact of the API I'm not always sure).


### Batching
It should be possible to do streaming in batches.  This is because, depending on the nature of the source and sink, data may be stored in memory when streaming,
which becomes a problem if it is too big for memory.  This is only a problem if internally `Data.streamfrom` involves some sort of buffering.  I haven't thought
this through yet, but I think it should involve the optional declaration of something like `Data.batchsize(src)`.  We should probably implement the above before
working on batching, but we should keep in mind that we ultimately want to be able to do it.


## Overhaul of `Data.streamto!`
This one is harder.  Here's my stab at it (again, typing handled by schemas, see below), users should define at least one of the following:

- `Data.streamto!(snk, row, col, val)`
- `Data.streamto!(snk, row::AbstractVector, col, val::AbstractVector)` obviously one should have `length(val) == length(row)`.
- `Data.streamto!(snk, col, val::AbstractVector)` obviously one should have `length(val) == size(snk, 1)`.

These are analogous to `Data.streamfrom`.

### Typing
When streaming one field at a time, a single `convert(Tsnk, val)` for each element is called in `Data.stream!` where `Tsnk` is the element type of the sink
column and `val` is the output of `Data.streamfrom`.  If this would fail, users are required to compensate for this using the `transforms` dictionary
(`DataStreams` won't make any further guesses about how to achieve the conversion).

When streaming one column (or partial column) at a time, a single `convert(Data.vectortype(snk, Tsnk), val)` is called in `Data.stream!` where `Tsnk` is the
element type of the sink column and `val` is the output (vector) from `Data.streamfrom`.  As with sources, `Data.vectortype(snk, Tsnk)` will default to
`Vector{Tsnk}` or `NullableVector{Tsnk}`.


### `Data.StreamType`
In most cases it probably makes sense to use the streaming type of the source.  I suppose in cases where we want the sink to have a different `StreamType` from
the source, we could impose some sort of buffering scheme, but I think we should put that on the back burner.

### Batching
Again, for now let's inherit any sort of batching from the source, eventually we can implement some sort of buffering scheme.


## Conclusion
Alright, that's my attempt.  Everything I didn't mention here I'd keep pretty much the same.  The three really crucial pieces of this (in my mind) are that we
1. allow partial column streaming, 
2. allow `Data.stream!` to cycle through either rows first or columns first and 
3. that we simplify the (currently very confusing) typing situation for the end users.
I'm sure those who wrote `DataStreams` in the first place will have lots of ideas about where this goes wrong (or perhaps it would go so wrong they'll reject it
outright).

