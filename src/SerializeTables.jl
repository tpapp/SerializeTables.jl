module SerializeTables

export serialize_table_rows, deserialize_table_rows

using ArgCheck: @argcheck
using DocStringExtensions: FUNCTIONNAME, SIGNATURES
using Serialization: serialize, deserialize, Serializer, Serialization
using Tables
using TranscodingStreams: TranscodingStream, Codec


# preamble

const SIGNATURE = "SerializedTables.jl table data"

struct Version{V}
    function Version{V}() where V
        @argcheck V isa Int64
        new{V}()
    end
end

const VERSION = Version{1}()

function write_preamble(io, ::Version{V}) where V
    write(io, SIGNATURE)
    write(io, V)
end

function checked_preamble_version(io)
    for c in Vector{UInt8}(SIGNATURE)
        @argcheck read(io, UInt8) == c "invalid signature"
    end
    Version{read(io, Int64)}()
end


# generic serialize and deserialize

"""
$(FUNCTIONNAME)([codec], filename, table; kwargs...)

Write representation of `table` (which conforms to the `Tables` interface and provides row
access) to `filename`, by rows, using Julia's serializer from the standard library.

When `codec` is specified, it is used with `TranscodingStreams` for compression.
"""
serialize_table_rows(filename::AbstractString, table; kwargs...) =
    open(io -> serialize_table_rows(io, table; kwargs...), filename, "w")

function serialize_table_rows(codec::Codec, filename::AbstractString, table; kwargs...)
    open(filename, "w") do io
        compressed_io = TranscodingStream(codec, io)
        serialize_table_rows(compressed_io, table; kwargs...)
        close(compressed_io)
    end
end

function serialize_table_rows(io::IO, table; kwargs...)
    write_preamble(io, VERSION)
    _serialize_table_rows(VERSION, io, table; kwargs...)
end

"""
$(SIGNATURES)

Return an iterable for the table rows in filename, that supports the rows interface of `Tables`.

When `codec` is specified, it is used with `TranscodingStreams` for decompression.
"""
deserialize_table_rows(filename::AbstractString) = # FIXME finalizer?
    deserialize_table_rows(open(filename, "r"))

deserialize_table_rows(codec::Codec, filename::AbstractString) =
    deserialize_table_rows(TranscodingStream(codec, open(filename, "r")))

function deserialize_table_rows(io::IO)
    V = checked_preamble_version(io)
    # FIXME finalizer?
    _deserialize_table_rows(V, io)
end


# Version 1

"""
$(SIGNATURES)

# Layout

A serialized stream of

1. the schema (of rows)
2. contents of rows, directly as named tuples.
"""
function _serialize_table_rows(V::Version{1}, io::IO, table)
    @argcheck Tables.istable(table)
    s = Serializer(io)
    Serialization.writeheader(s)
    rows = Tables.rows(table)
    serialize(s, Tables.schema(rows))
    for row in rows
        serialize(s, row)
    end
end

mutable struct SerializedRows{S,I,T}
    schema::S
    io::I
    serializer::T
    atstart::Bool
end

_close(sr::SerializedRows) = close(sr.io)

function _deserialize_table_rows(::Version{1}, io::IO)
    serializer = Serializer(io)
    schema = deserialize(serializer)
    SerializedRows(schema, io, serializer, true)
end

Tables.istable(::Type{<:SerializedRows}) = true

Tables.rowaccess(::Type{<:SerializedRows}) = true

Tables.schema(sr::SerializedRows) = sr.schema

Tables.rows(sr::SerializedRows) = sr

Base.IteratorSize(::Type{<:SerializedRows}) = Base.SizeUnknown()

function Base.iterate(sr::SerializedRows, atstart = true)
    if atstart
        @argcheck sr.atstart "Iteration in progress, open file again."
        sr.atstart = false
    end
    eof(sr.io) && (_close(sr); return nothing)
    deserialize(sr.serializer), false
end

end # module
