module SerializeTables

export serialize_table_rows, deserialize_table_rows

using ArgCheck: @argcheck
using CodecZlib: GzipCompressor, GzipDecompressorStream
using DocStringExtensions: SIGNATURES
using Serialization: serialize, deserialize, Serializer, Serialization
using Tables
using TranscodingStreams: TranscodingStream


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
$(SIGNATURES)

Write representation of `table` (which conforms to the `Tables` interface and provides row
access) to `filename`, by rows.
"""
function serialize_table_rows(filename, table; kwargs...)
    open(filename, "w") do io
        write_preamble(io, VERSION)
        _serialize_table_rows(VERSION, io, table; kwargs...)
    end
end

"""
$(SIGNATURES)

Return an iterable for the table rows in filename, also supports `Tables.schema`.
"""
function deserialize_table_rows(filename)
    io = open(filename, "r")
    V = checked_preamble_version(io)
    # TODO finalizer
    _deserialize_table_rows(V, io)
end


# Version 1

const DEFAULT_COMPRESSION_LEVEL = 5

function write_header(::Version{1}, io, schema)
    s = Serializer(io)
    Serialization.writeheader(s)
    serialize(s, schema)
    nothing
end

function write_compressed_rows(::Version{1}, io, rows, codec)
    compressed_io = TranscodingStream(codec, io)
    s = Serializer(compressed_io)
    for row in rows
        serialize(s, row)
    end
    close(compressed_io)
end

"""
$(SIGNATURES)

# Layout of header + data

## Header

1. the schema (of rows), serialized
3. contents of rows, serialized directly as named tuples, Gzip compressed.
"""
function _serialize_table_rows(V::Version{1}, io::IO, table;
                               compression_level = DEFAULT_COMPRESSION_LEVEL)
    @argcheck Tables.istable(table)
    rows = Tables.rows(table)
    write_header(V, io, Tables.schema(rows))
    write_compressed_rows(V, io, rows, GzipCompressor(; level = compression_level))
end

mutable struct SerializedRows{S,I,C,T}
    schema::S
    io::I
    compressed_io::C
    serializer::T
    atstart::Bool
end

_close(sr::SerializedRows) = (close(sr.compressed_io); close(sr.io))

function _deserialize_table_rows(::Version{1}, io::IO)
    schema = deserialize(io)
    compressed_io = GzipDecompressorStream(io)
    serializer = Serializer(compressed_io)
    SerializedRows(schema, io, compressed_io, serializer, true)
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
    eof(sr.compressed_io) && (_close(sr); return nothing)
    deserialize(sr.serializer), false
end

end # module
