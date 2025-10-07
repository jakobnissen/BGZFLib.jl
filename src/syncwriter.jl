# TODO: Functionality to make index while writing?

mutable struct SyncBGZFWriter{T <: AbstractBufWriter} <: AbstractBufWriter
    const io::T
    const buffer::Memory{UInt8}
    compressor::Union{Nothing, Compressor}
    # 1:n_flushed is flushed
    # n_flushed+1:n_filled is consumed and unflushed
    # n_filled+1:end is unconsumed
    n_flushed::Int
    n_filled::Int

    append_empty::Bool
end

function SyncBGZFWriter(io::AbstractBufWriter; append_empty::Bool = true, compresslevel::Int = 6)
    get_writer_sink_room(io)
    buffer = Memory{UInt8}(undef, 4 * MAX_BLOCK_SIZE)
    return SyncBGZFWriter{typeof(io)}(io, buffer, Compressor(compresslevel), 0, 0, append_empty)
end

function SyncBGZFWriter(io::IO; append_empty::Bool = true, compresslevel::Int = 6)
    buf = BufWriter(io, 2 * MAX_BLOCK_SIZE)
    return SyncBGZFWriter(buf; append_empty, compresslevel)
end

BufferIO.get_buffer(io::SyncBGZFWriter) = MemoryView(io.buffer)[(io.n_filled + 1):end]
BufferIO.get_unflushed(io::SyncBGZFWriter) = MemoryView(io.buffer)[(io.n_flushed + 1):io.n_filled]

function BufferIO.consume(io::SyncBGZFWriter, n::Int)
    @boundscheck if (n % UInt) > (length(io.buffer) - io.n_filled) % UInt
        throw(IOError(IOErrorKinds.ConsumeBufferError))
    end
    return io.n_filled += n
end

function BufferIO.grow_buffer(io::SyncBGZFWriter)::Int
    check_open(io)

    # If buffer is full, don't grow, but return zero
    iszero(io.n_filled) && return 0

    # Copy data around to make room, if possible
    n_flushed = io.n_flushed
    if n_flushed > 0
        n_bytes = io.n_filled - n_flushed
        copy!(MemoryView(io.buffer)[1:n_bytes], get_unflushed(io))
        io.n_filled = n_bytes
        io.n_flushed = 0
        return n_flushed
    end

    # Else, flush all full blocks (and at least one block).
    total_flushed = 0
    while iszero(total_flushed) || io.n_filled - io.n_flushed ≥ SAFE_DECOMPRESSED_SIZE
        total_flushed += flush_chunk(io)
    end
    return total_flushed
end

function Base.close(io::SyncBGZFWriter)
    shallow_flush(io)
    io.append_empty && write(io.io, EOF_BLOCK)
    close(io.io)
    io.compressor = nothing
    return nothing
end

Base.isopen(io::SyncBGZFWriter) = io.compressor !== nothing

function check_open(io::SyncBGZFWriter)
    return isopen(io) || error("Operation on closed SyncBGZFWriter") # TODO: Proper error
end

# Should not be empty.
function flush_chunk(io::SyncBGZFWriter)::Int
    src = get_unflushed(io)
    mn = min(length(src), SAFE_DECOMPRESSED_SIZE)
    src = @inbounds src[1:mn]
    dst = get_writer_sink_room(io.io)
    # 12 bytes header + 6 bytes for BC block
    header_offset = 12 + 6
    GC.@preserve src dst begin
        libdeflate_return = unsafe_compress!(
            something(io.compressor),
            pointer(dst) + header_offset,
            length(dst) - (header_offset + 8),
            pointer(src),
            length(src),
        )
    end
    if libdeflate_return isa LibDeflateError
        # TODO: Add file_offset
        throw(BGZFError(nothing, libdeflate_return))
    end
    GC.@preserve src begin
        crc32 = unsafe_crc32(pointer(src), length(src))
    end
    block_size = UInt16(header_offset + libdeflate_return + 8)
    copyto!(dst, ImmutableMemoryView(BLOCK_HEADER))
    unsafe_bitstore!(block_size - 0x01, dst, 17)
    unsafe_bitstore!(crc32, dst, block_size - 7)
    unsafe_bitstore!(length(src) % UInt32, dst, block_size - 3)
    write(io.io, dst[1:block_size])
    io.n_flushed += length(src)
    return length(src)
end

"""
    write_empty_block(io::SyncBGZFWriter)::Int

Perform a `shallow_flush`, then write an empty block.
Return the number of bytes flushed.
"""
function write_empty_block(io::SyncBGZFWriter)
    n = shallow_flush(io)
    write(io.io, EOF_BLOCK)
    return n
end

function BufferIO.shallow_flush(io::SyncBGZFWriter)::Int
    check_open(io)
    n = 0
    while io.n_flushed < io.n_filled
        n += flush_chunk(io)
    end
    return n
end

function Base.flush(io::SyncBGZFWriter)
    shallow_flush(io)
    flush(io.io)
    return nothing
end
