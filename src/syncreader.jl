"""
    SyncBGZFReader(io::T <: IO; check_truncated::Bool=true)::SyncBGZFReader{BufReader{T}}
    SyncBGZFReader(io::T <: AbstractBufReader; check_truncated::Bool=true)::SyncBGZFReader{T}

Create a `SyncBGZFReader <: AbstractBufReader` that decompresses BGZF files.

When constructing from an `io::AbstractBufReader`, `io` must have a buffer size of at least
$(MAX_BLOCK_SIZE), or be able to grow its buffer to this size.

If `check_truncated`, the last BGZF block in the file must be empty, otherwise the reader
throws an error. This can be used to detect the file was truncated.

Unlike `BGZFReader`, the decompression happens in in serial in the main task.
This is slower and does not enable paralellism, but may be preferable in situations
where task scheduling or contention is an issue.

If the reader encounters an error, it goes into an error state and throws an exception.
The reader can be reset by using `seek` or `seekstart`. A closed reader cannot be reset.
"""
mutable struct SyncBGZFReader{T <: AbstractBufReader} <: AbstractBufReader
    const io::T
    const gzip_extra_fields::Vector{GzipExtraField}
    const buffer::Memory{UInt8}
    decompressor::Union{Nothing, Decompressor} # nothing when closed
    start::Int
    stop::Int
    n_bytes_read::Int
    current_block_size::UInt32
    const check_truncated::Bool
    last_was_empty::Bool
    state::UInt8
end

function SyncBGZFReader(io::AbstractBufReader; check_truncated::Bool = true)
    return SyncBGZFReader{typeof(io)}(
        io,
        GzipExtraField[],
        Memory{UInt8}(undef, MAX_BLOCK_SIZE),
        Decompressor(),
        1,
        0,
        0,
        0,
        check_truncated,
        false,
        STATE_OPEN,
    )
end

function SyncBGZFReader(io::IO; check_truncated::Bool = true)
    bufio = BufReader(io, BUFREADER_BUFFER_SIZE)
    return SyncBGZFReader(bufio; check_truncated)
end

BufferIO.get_buffer(io::SyncBGZFReader) = @inbounds ImmutableMemoryView(io.buffer)[io.start:io.stop]

function BufferIO.consume(io::SyncBGZFReader, n::Int)
    @boundscheck if (n % UInt) > (io.stop - io.start + 1) % UInt
        throw(IOError(IOErrorKinds.ConsumeBufferError))
    end
    io.start += n
    return nothing
end

Base.isopen(io::SyncBGZFReader) = io.state == STATE_OPEN

function throw_error(io::SyncBGZFReader, err::BGZFError)
    io.start = 1
    io.stop = 0
    io.state = STATE_ERROR
    throw(err)
end

function Base.close(io::SyncBGZFReader)
    isopen(io) || return nothing
    io.start = 1
    io.stop = 0
    empty!(io.gzip_extra_fields)
    io.decompressor = nothing
    close(io.io)
    io.state = STATE_CLOSED
    return nothing
end

"""
    virtual_position(io::Union{SyncBGZFReader, BGZFReader})::VirtualOffset

Get the `VirtualOffset` of the current BGZF reader. The virtual offset is a
position in the decompressed stream. Seek to the position using `virtual_seek`.
"""
function virtual_position(io::SyncBGZFReader)
    return VirtualOffset(io.n_bytes_read - io.current_block_size, io.start - 1)
end

"""
    virtual_seek(io::Union{SyncBGZFReader, BGZFReader}, vo::VirtualOffset) -> io

Seek to the virtual position `vo`. The virtual position is usually obtained by
a call to `virtual_position`.
"""
function virtual_seek(io::SyncBGZFReader, vo::VirtualOffset)
    seek(io, Int(vo.file_offset % Int))
    fill_buffer(io)
    if io.stop < vo.block_offset
        throw(BGZFError(vo.file_offset % Int, BGZFErrors.inblock_offset_out_of_bounds))
    end
    io.start += vo.block_offset
    return io
end

function Base.seek(io::SyncBGZFReader, offset::Int)
    seek(io.io, offset)
    io.stop = 0
    io.start = 1
    io.last_was_empty = false
    io.n_bytes_read = offset
    io.state = STATE_OPEN
    return io
end

function BufferIO.fill_buffer(io::SyncBGZFReader)
    io.state == STATE_CLOSED && return 0
    io.state == STATE_ERROR && error("Reader is in an error state. Use `seek` or `virtual_seek` to reset it")

    io.stop > io.start && return nothing
    io.start = 1
    io.stop = 0
    last_was_empty = io.check_truncated ? nothing : io.last_was_empty
    (; consumed, result) = get_reader_block_work(io.io, io.gzip_extra_fields, last_was_empty, io.n_bytes_read)
    io.n_bytes_read += consumed
    if result === nothing
        io.last_was_empty = true
        # An empty block is 28 bytes: 12 bytes header + 6 bytes extra data +
        # 2 bytes for DEFLATE compression of empty payload + 8 bytes for crc32
        # and decompressed length, both as UInt32.
        io.current_block_size = 28
        return 0
    elseif result isa BGZFError
        throw_error(io, result)
    else
        io.last_was_empty = false
        (; payload, block_size, decompressed_len, expected_crc32) = result
        io.current_block_size = block_size
        destination = io.buffer
        GC.@preserve payload destination begin
            libdeflate_return = unsafe_decompress!(
                Base.HasLength(),
                something(io.decompressor),
                pointer(destination),
                decompressed_len,
                pointer(payload),
                length(payload),
            )
        end
        if libdeflate_return isa LibDeflateError
            throw_error(io, BGZFError(io.n_bytes_read, libdeflate_return))
        else
            GC.@preserve destination begin
                crc32 = unsafe_crc32(pointer(destination), decompressed_len)
            end
            if crc32 != expected_crc32
                throw_error(io, BGZFError(io.n_bytes_read, LibDeflateErrors.gzip_bad_crc32))
            end
        end
        io.stop = decompressed_len
        consume(io.io, Int(block_size))
        io.n_bytes_read += block_size
        return decompressed_len % Int
    end
end
