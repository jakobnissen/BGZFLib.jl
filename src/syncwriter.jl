"""
    SyncBGZFWriter(io::T <: AbstractBufWriter; kwargs)::SyncBGZFWriter{T}
    SyncBGZFWriter(io::T <: IO; kwargs)::SyncBGZFWriter{BufWriter{T}}

Create a `SyncBGZFWriter <: AbstractBufWriter` that writes compresses data written to it,
and writes the compressed BGZF file to the underlying `io`.

This type differs from `BGZFWriter` in that it does the compression serial in the main task.
Therefore it is slower when multiple threads are present, but does not incur Task- and scheduling
overhead.

If `io::AbstractBufWriter`, `io` must be able to buffer up to 2^16 bytes, else a
`BGZFError(nothing, BGZFErrors.insufficient_writer_space)` is thrown.

The keyword arguments are:
* `compresslevel::Int`: Set compression level from 1 to 12, with 12 being slowest but with
  the best compression ratio. It defaults to an intermediate level of compression.
* `append_empty::Bool = true`. If set, closing the `SyncBGZFWriter` will write an empty BGZF block,
  indicating EOF.
"""
mutable struct SyncBGZFWriter{T <: AbstractBufWriter} <: AbstractBufWriter
    const io::T
    const buffer::Memory{UInt8}
    compressor::Union{Nothing, Compressor}
    # 1+1:n_filled is consumed and unflushed
    # n_filled+1:end is unconsumed
    n_filled::Int
    append_empty::Bool
end

function SyncBGZFWriter(io::AbstractBufWriter; append_empty::Bool = true, compresslevel::Int = 6)
    # Ensure underlying writer has room for a full block. This is necessary, because we compress
    # straight to `io`'s buffer.
    get_writer_sink_room(io)
    buffer = Memory{UInt8}(undef, SAFE_DECOMPRESSED_SIZE)
    return SyncBGZFWriter{typeof(io)}(io, buffer, Compressor(compresslevel), 0, append_empty)
end

function SyncBGZFWriter(io::IO; append_empty::Bool = true, compresslevel::Int = 6)
    buf = BufWriter(io, MAX_BLOCK_SIZE)
    return SyncBGZFWriter(buf; append_empty, compresslevel)
end

function SyncBGZFWriter(f, args...; kwargs...)
    reader = SyncBGZFWriter(args...; kwargs...)
    return try
        f(reader)
    finally
        close(reader)
    end
end

BufferIO.get_buffer(io::SyncBGZFWriter) = MemoryView(io.buffer)[(io.n_filled + 1):end]
BufferIO.get_unflushed(io::SyncBGZFWriter) = MemoryView(io.buffer)[1:io.n_filled]

function BufferIO.consume(io::SyncBGZFWriter, n::Int)
    @boundscheck if (n % UInt) > (length(io.buffer) - io.n_filled) % UInt
        throw(IOError(IOErrorKinds.ConsumeBufferError))
    end
    return io.n_filled += n
end

function BufferIO.grow_buffer(io::SyncBGZFWriter)::Int
    check_open(io)

    # The type does not support actually growing the underlying buffer.
    # Hence, if buffer is full (i.e. has no filled bytes), don't grow but return zero.
    iszero(io.n_filled) && return 0

    # Else, flush to make room.
    return compress_and_flush_buffer(io)
end

function Base.close(io::SyncBGZFWriter)
    shallow_flush(io)
    io.append_empty && write(io.io, EOF_BLOCK)
    flush(io.io)
    close(io.io)
    io.compressor = nothing
    return nothing
end

Base.isopen(io::SyncBGZFWriter) = io.compressor !== nothing

function check_open(io::SyncBGZFWriter)
    return isopen(io) || throw(IOError(IOErrorKinds.ClosedIO))
end

function compress_and_flush_buffer(io::SyncBGZFWriter)::Int
    src = get_unflushed(io)
    isempty(src) && return 0
    @assert length(src) ≤ SAFE_DECOMPRESSED_SIZE
    dst = get_writer_sink_room(io.io)
    compress_result = compress_block!(dst, ImmutableMemoryView(src), something(io.compressor))
    if compress_result isa LibDeflateError
        throw(BGZFError(nothing, compress_result))
    end
    write(io.io, dst[1:compress_result])
    io.n_filled = 0
    return length(src)
end

"""
    write_empty_block(io::Union{SyncBGZFWriter, BGZFWriter})

Perform a shallow_flush, then write an empty block.

# Examples
Write the final empty EOF block manually:

```jldoctest
julia> io = VecWriter();

julia> SyncBGZFWriter(io; append_empty=false) do writer
           write(writer, "Hello")
           # Manually write the empty EOF block
           write_empty_block(writer)
      end

julia> SyncBGZFReader(CursorReader(io.vec); check_truncated=true) do reader
           read(reader, String)
       end
"Hello"
```
"""
function write_empty_block(io::SyncBGZFWriter)
    shallow_flush(io)
    write(io.io, EOF_BLOCK)
    return nothing
end

function BufferIO.shallow_flush(io::SyncBGZFWriter)::Int
    check_open(io)
    return iszero(io.n_filled) ? 0 : compress_and_flush_buffer(io)
end

function Base.flush(io::SyncBGZFWriter)
    shallow_flush(io)
    flush(io.io)
    return nothing
end
