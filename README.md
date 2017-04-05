# Gradines

A Julia package for handling serialized tabular data.

The original idea behind this package had three meain goals:
- Be able to access tabular data which has been saved to disk, while only loading arbitrarily small pieces into memory with the full functionality of a
    data-frame interface.
- Allow stored tabular data to be extended arbitrarily (sadly, this currently only supports a fixed number of rows).
- Use a simple "universal" format that can be easily loaded using Julia, Python or C (in principle this is still possible since I'm using HDF5, but I've largely
    abandoned this due to lack of good options).

Currenlty I'm using [JLD](https://github.com/JuliaIO/JLD) as a backend, which is a Julia-specific implementation of HDF5.

The intention is that what I have here be combined with methods for easily converting data into a format useful for machine-learning (probably a more
sophisticated version of my package [DataHandlers](https://github.com/ExpandingMan/DataHandlers)).

