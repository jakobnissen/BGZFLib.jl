module BGZFLib

const DEBUG = true

using MemoryViews: ImmutableMemoryView, MutableMemoryView, MemoryView
using LibDeflate: ReadableMemory,
    Compressor,
    Decompressor,
    GzipExtraField,
    unsafe_parse_gzip_header,
    LibDeflateError,
    LibDeflateErrors

using BufIO: BufIO

export VirtualOffset, BGZFReader, BGZFError

# BGZF blocks are no larger than 64 KiB before and after compression.
const MAX_BLOCK_SIZE = UInt(64 * 1024)

# Blocks can be idle, running, done or error
# Reader can be idle, error or closed
const STATE_IDLE = 0x00
const STATE_RUNNING = 0x01
const STATE_DONE = 0x02
const STATE_ERROR = 0x03
const STATE_CLOSED = 0x04

module BGZFErrors

    @enum BGZFErrorType::UInt8 begin
        TruncatedFile
        MissingBCField
        TooLargeBlockSize
        ClosedStream
        VOTooHighBlock
        VOOutOfBlock
    end

    export BGZFErrorType

end # module BGZFErrors

using .BGZFErrors

struct BGZFError <: Exception
    type::Union{BGZFErrorType, LibDeflateError}
end

const BitInteger = Union{UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64}

# By specs, BAM files are always little-endian
function unsafe_bitload(T::Type{<:BitInteger}, data::ImmutableMemoryView{UInt8}, p::Integer)
    return GC.@preserve data ltoh(unsafe_load(Ptr{T}(pointer(data, p))))
end

# By specs, BAM files are always little-endian
function unsafe_bitstore!(v::BitInteger, data::ImmutableMemoryView{UInt8}, p::Integer)
    return GC.@preserve data unsafe_store!(Ptr{typeof(v)}(pointer(data, p)), htol(v))
end

const DE_COMPRESSOR = Union{Compressor, Decompressor}

include("block.jl")

import .Blocks: Blocks, EOF_BLOCK

include("virtualoffset.jl")

mutable struct BGZFReader{I <: IO} <: BufIO.AbstractBufReader
    const io::I

    # This is not const, because we replace it with an empty memory once the reader is closed
    blocks::Memory{Blocks.Block{Decompressor}}

    # We need these to parse in gzip header, to find the BC field
    # in order to know the block size
    const gzip_extra_fields::Vector{GzipExtraField}

    # Number of bytes read from the underlying IO.
    # Used to report position.
    n_bytes_read::Int

    # Read from this index in the current block
    read_index::UInt32

    # Starting index of buffer
    buffer_start::UInt32

    # Current block to read from
    reading_block_index::UInt32

    # Block to write data to next.
    writing_block_index::UInt32

    # If `reader.io` has been seen to be eof, we set this.
    # We unset it automatically if the underlying IO un-EOFs
    underlying_is_eof::Bool

    # Check for an empty BGZF block at EOF, and throw an error
    # if not found
    check_truncated::Bool

    state::UInt8

    error::BGZFError
end

function error!(reader::BGZFReader, error::BGZFError)::Union{}
    reader.state = STATE_ERROR
    reader.error = error
    throw(error)
end

next_block_index(r::BGZFReader, i::Int) = i ≥ length(r.blocks) ? 1 : i + 1

function BGZFReader(
        io::IO;
        threads::Int = Threads.nthreads(),
        check_truncated::Bool = true
    )
    threads < 1 && throw(ArgumentError("Must use at least one thread to read"))
    blocks = Memory{Blocks.Block{Decompressor}}(undef, threads)
    for i in eachindex(blocks)
        blocks[i] = Blocks.Block(Decompressor())
    end
    return BGZFReader{typeof(io)}(
        io,
        blocks,
        sizehint!(GzipExtraField[], 4),
        0,
        1,
        1,
        1,
        1,
        false,
        check_truncated,
        STATE_IDLE,
        BGZFError(BGZFErrors.TruncatedFile), # arbitrary value
    )
end

function Base.close(reader::BGZFReader)
    reader.state == STATE_CLOSED && return nothing
    reader.state = STATE_CLOSED
    reader.error = BGZFError(BGZFErrors.ClosedStream)
    # Free underlying resources
    reader.blocks = Memory{Blocks.Block{Decompressor}}()
    return close(reader.io)
end

function Base.eof(x::BGZFReader)
    x.state == STATE_CLOSED && return true
    isempty(BufIO.get_buffer(x)) || return false
    # This can't be nothing, because that should only happen
    # if the current buffer is not empty.
    return iszero(something(BufIO.fill_buffer(x)))
end

Base.isopen(reader::BGZFReader) = reader.state != STATE_CLOSED
Base.iswritable(::BGZFReader) = false

function Base.reset(reader::BGZFReader, threads::Int)
    threads < 1 && throw(ArgumentError("Must reset with at least 1 thread"))
    seekstart(reader.io)
    return reset_no_seek(reader, threads)
end

function reset_no_seek(reader::BGZFReader, threads::Int)
    seekstart(reader.io)
    if isempty(reader.blocks)
        blocks = Memory{Blocks.Block{Decompressor}}(undef, threads)
        for i in eachindex(blocks)
            blocks[i] = Blocks.Block(Decompressor())
        end
        reader.blocks = blocks
    else
        for block in reader.blocks
            reset(block)
        end
    end
    reader.state = STATE_IDLE
    reader.n_bytes_read = 0
    reader.buffer_start = 1
    reader.reading_block_index = 1
    reader.writing_block_index = 1
    reader.underlying_is_eof = false
    return reader
end

function Base.seek(reader::BGZFReader, v::VirtualOffset)
    reader.state == STATE_CLOSED && throw(BGZFError(BGZFErrors.ClosedStream))
    reset_no_seek(reader, 0)
    (block_offset, inblock_offset) = offsets(v)
    seek(reader.io, block_offset)
    BufIO.fill_buffer(reader)
    block = reader.blocks[reader.reading_block_index]
    if block.out_len < inblock_offset
        error!(reader, BGZFError(BGZFErrors.VOOutOfBlock))
    end
    return BufIO.consume(reader, Int(inblock_offset))
end

function Base.position(reader::BGZFReader)
    block = reader.blocks[reader.reading_block_index]
    return VirtualOffset(
        block.file_offset,
        reader.read_index - 1
    )
end

function BufIO.get_buffer(reader::BGZFReader)
    reader.state == STATE_IDLE || throw(reader.error)
    block = @inbounds reader.blocks[reader.reading_block_index]
    return @inbounds ImmutableMemoryView(block.out_data)[reader.read_index:block.out_len]
end

function get_bsize(
        fields::Vector{GzipExtraField},
        data::ImmutableMemoryView{UInt8}
    )::Union{UInt16, Nothing}
    fieldnum = findfirst(fields) do field
        field.tag === (UInt8('B'), UInt8('C'))
    end
    fieldnum === nothing && return nothing
    field = @inbounds fields[fieldnum]
    field.data === nothing && return nothing
    length(field.data) != 2 && return nothing
    return (data[first(field.data)] % UInt16) | ((data[last(field.data)] % UInt16) << 8)
end

# Copy from the underlying IO into the buffer, and queue decompression,
# for as many blocks as possible
# Set reader.underlying_is_eof as appropriate, and set writing_block_index to
# the first non-idle block, or whereever we reached if EOF.
# Return whether any block was queued
function queue!(reader::BGZFReader, ::Val{is_sync})::Bool where {is_sync}
    blocks = reader.blocks
    last_block_was_empty = false

    # Sync means we only use one block
    @assert is_sync ⊻ (length(blocks) > 1)

    # We queue in a loop, and when we hit EOF we need to check if we
    # have queued in a previous loop when exiting
    queued_any = false

    while true
        block = blocks[reader.writing_block_index]

        # We only queue idle blocks.
        state = @atomic :acquire block.state
        if !queued_any
            # If this is the first block to queue, the block must be idle.
            # I.e. we never call `queue!` when there can be zero blocks to queue.
            @assert state == STATE_IDLE
        end
        state == STATE_IDLE || return queued_any

        n_decompressed = fill_writing_block!(reader, block)
        if isnothing(n_decompressed)
            # This indicates EOF. Check the last block was empty - the BAM EOF marker.
            # Note that since we skip empty blocks, this is guaranteed to occur in
            # this same call too `queue!` in a previous loop.
            if !reader.underlying_is_eof && reader.check_truncated && !last_block_was_empty
                error!(reader, BGZFError(BGZFErrors.TruncatedFile))
            end

            # Set this such that if we continuously try to read when EOF, we don't
            # accidentally hit the EOF check again.
            reader.underlying_is_eof = true
            return queued_any
        end

        if iszero(n_decompressed)
            # Don't try to decompress empty files.
            last_block_was_empty = true
            continue
        else
            last_block_was_empty = false
        end

        # If one block, queue and return (since there are no more blocks to queue)
        if is_sync
            Blocks.queue_sync!(block)
            return true
        end

        Blocks.queue!(block)
        queued_any = true

        # Move to next block to queue
        reader.writing_block_index = next_block_index(reader, Int(reader.writing_block_index))
    end
    return false # unreachable
end

function read_all!(buffer::MutableMemoryView{UInt8}, io::IO)::Int
    n_read_total = 0
    while !isempty(buffer)
        n_read = readbytes!(io, buffer)
        iszero(n_read) && return n_read_total
        n_read_total += n_read
        buffer = @inbounds buffer[(n_read + 1):end]
    end
    return n_read_total
end

# Returns number of decompressed bytes, or nothing if EOF
function fill_writing_block!(reader::BGZFReader, block::Blocks.Block{Decompressor})::Union{Nothing, Int}
    buffer = MemoryView(block.in_data)
    offset_at_block_start = reader.n_bytes_read

    n_read = read_all!(@inbounds(buffer[1:12]), reader.io)
    reader.n_bytes_read += n_read
    iszero(n_read) && return nothing
    n_read == 12 || error!(reader, BGZFError(BGZFErrors.TruncatedFile))

    ex_len = (@inbounds buffer[11] % Int) | ((@inbounds buffer[12] % Int) << 8)
    n_read = read_all!(@inbounds(buffer[13:(ex_len + 12)]), reader.io)
    reader.n_bytes_read += n_read
    n_read == ex_len || error!(reader, BGZFError(BGZFErrors.TruncatedFile))

    extra_fields = reader.gzip_extra_fields
    GC.@preserve buffer begin
        parsed_header = unsafe_parse_gzip_header(pointer(buffer), (12 + ex_len) % UInt, extra_fields)
    end

    parsed_header isa LibDeflateError && error!(reader, BGZFError(parsed_header))

    fieldnum = findfirst(extra_fields) do field
        field.tag === (UInt8('B'), UInt8('C'))
    end
    fieldnum === nothing && error!(reader, BGZFError(BGZFErrors.MissingBCField))

    field = @inbounds extra_fields[fieldnum]
    field.data === nothing && return error!(reader, BGZFError(BGZFErrors.MissingBCField))
    length(field.data) != 2 && return error!(reader, BGZFError(BGZFErrors.MissingBCField))

    bc = (buffer[first(field.data)] % UInt16) | ((buffer[last(field.data)] % UInt16) << 8)

    # Block size is bc + 1
    # We already read ex_len + 12 bytes
    # So remaining is bc + 1 - ex_len - 12
    rest_len = bc + 1 - ex_len - 12
    n_read = read_all!(@inbounds(buffer[1:rest_len]), reader.io)
    reader.n_bytes_read += n_read
    n_read == rest_len || error!(reader, BGZFError(BGZFErrors.TruncatedFile))

    compressed_payload_len = rest_len - 8
    target_crc32 = unsafe_bitload(UInt32, ImmutableMemoryView(buffer), rest_len - 7)
    decompressed_payload_len = unsafe_bitload(UInt32, ImmutableMemoryView(buffer), rest_len - 3)

    block.crc32 = target_crc32
    block.out_len = decompressed_payload_len
    block.in_len = compressed_payload_len
    block.file_offset = offset_at_block_start

    return decompressed_payload_len
end

function BufIO.fill_buffer(reader::BGZFReader)::Union{Nothing, Int}
    # Check reader state is correct
    reader.state == STATE_CLOSED && throw(ArgumentError("Operation on closed BGZFReader"))
    reader.state == STATE_ERROR && throw(reader.error)

    blocks = reader.blocks
    block = @inbounds blocks[reader.reading_block_index]

    state = @atomic :acquire block.state
    # State can be idle (if first call, or if repeated calls after EOF)
    # or done.
    # It cannot be running, because when moving the reader block to a
    # running block, it will immediate wait.
    # It can also not be error, because moving to an error block will propagate
    # the error to the reader
    if state == STATE_DONE
        # If the block already has data, we can't fill the buffer.
        reader.read_index ≤ block.out_len && return nothing

        # Else, reset the block
        reader.read_index = 1
        block.out_len = 0
        @atomic :release block.state = STATE_IDLE

        # Move to the next block
        next_index = next_block_index(reader, Int(reader.reading_block_index))
        reader.reading_block_index = next_index
        block = @inbounds blocks[next_index]
        state = @atomic :acquire block.state
    else
        @assert state == STATE_IDLE
    end

    # If there's only one block, it's simple: Queue the block and check if it
    # has data
    if length(blocks) == 1
        # If the block couldn't be queued, it must be because of EOF
        queue!(reader, Val{true}()) || return 0
        state == STATE_ERROR && error!(reader, BGZFError(block.error))
        return block.out_len % Int
    end

    # If the underlying IO is EOF, we defer queuing until we run out of
    # buffered data. This way, the underlying stream has a chance to
    # un-EOF, if it has that option.
    was_eof_at_first = reader.underlying_is_eof
    was_eof_at_first || queue!(reader, Val{false}())

    # It's possible the next block was queued, so wait for it and update the state
    state = wait(block)

    if state == STATE_DONE
        @assert !iszero(block.out_len)
        return block.out_len
    elseif state == STATE_ERROR
        error!(reader, BGZFError(block.error))
    end

    # If we reached this point, then we've moved the reader block into the next
    # idle block (which is the writing block).
    @assert state == STATE_IDLE
    @assert reader.writing_block_index == reader.reading_block_index

    # State is idle, so either...
    if was_eof_at_first
        # We didn't queue before. Now queue, and if none could be queued,
        # we are EOF
        queue!(reader, Val{false}()) || return 0
    else
        # Or else we did queue before, but hit EOF
        return 0
    end

    # If we reach this point, we just queued the reading block,
    # and possibly more blocks. Wait for the first block
    state = wait(block)

    if state == STATE_DONE
        @assert !iszero(block.out_len)
        return block.out_len
    elseif state == STATE_ERROR
        error!(reader, BGZFError(block.error))
    else
        @assert false # unreachable
    end
end

function BufIO.consume(reader::BGZFReader, n::Int)
    reader.state == STATE_CLOSED && throw(ArgumentError("Operation on closed BGZFReader"))
    buf = BufIO.get_buffer(reader)
    length(buf) < n && error("Consumed too much")
    reader.read_index = (reader.read_index + n) % UInt32
    return nothing
end

#=
mutable struct BGZFWriter{I <: IO} <: BufIO.AbstractBufWriter
    const io::IO
    const blocks::Memory{Blocks.Block{Compressor}}

    # Index of block next write happens to
    index_of_block::UInt32

    # Next write happens to this index of the block
    index_in_block::UInt32

    # Index of next block to flush once current write block is full.
    flush_block_index::UInt32

    write_eof_on_close::Bool
end

function BufIO.get_buffer(io::BGZFWriter)::MutableMemoryView{UInt8}
    block = io.blocks[io.index_of_block]
    return MemoryView(block.in_data)[io.index_in_block:Blocks.SAFE_UNCOMPRESSED_BLOCK_SIZE]
end

function BufIO.consume(io::BGZFWriter, n::Int)::Nothing
    remaining = Blocks.SAFE_UNCOMPRESSED_BLOCK_SIZE = io.index_in_block + 1
    n > remaining && error("Consumed too much")
    io.index_in_block += n % UInt32
    return nothing
end

function BufIO.fill_buffer(io::BGZFWriter)::Int
    block = io.blocks[io.index_of_block]
    # Queue the current block
    queue!(block)
    io.index_in_block = 1

    # If one block, wait for it to compress, then ship it out
    if length(io.blocks) == 1
        wait(block.task)
        write(io.io, ImmutableMemoryView(block.out_data)[1:block.out_len])
        block.in_len = 0
    else
        # If more, wait for any done blocks and flush them. This loop does not
        # wait for blocks to finish.
        while true
            block = io.blocks[io.flush_block_index]
            if istaskdone(block.task)
                write(io.io, ImmutableMemoryView(block.out_data)[1:block.out_len])
                block.in_len = 0
                # If we wrote reached the block we just queued, we are done
                should_break = io.flush_block_index == io.index_of_block
                io.flush_block_index = next_block_index(io, io.flush_block_index % Int)
                should_break && break
            else
                # Else, if we need to wait, we are also done
                break
            end
        end
    end

    # Move to next block
    io.index_of_block = next_block_index(io, io.index_of_block % Int)

    # The block we just moved to must be empty. If it isn't, we need to wait
    # for it to finish compressing, and write it out.
    block = io.blocks[io.index_of_block]
    if !iszero(block.in_len)
        wait(block.task)
        write(io.io, ImmutableMemoryView(block.out_data)[1:block.out_len])
        block.in_len = 0
    end

    return Blocks.SAFE_UNCOMPRESSED_BLOCK_SIZE
end

function write(io::BGZFWriter, bytes::ImmutableMemoryView{UInt8})::Int
    # Write to current write block
    # Once block is exhausted, flush, then get next available block
end
=#

end # module BGZFLib
