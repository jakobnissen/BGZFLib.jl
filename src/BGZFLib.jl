module BGZFLib

const DEBUG = true

using MemoryViews: ImmutableMemoryView, MutableMemoryView, MemoryView
using LibDeflate: ReadableMemory,
    Compressor,
    Decompressor,
    GzipExtraField,
    parse_gzip_header,
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

    @enum BGZFErrorType::UInt8 TruncatedFile MissingBCField TooLargeBlockSize

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

    # We read from `io` in here.
    const buffer::Memory{UInt8}

    const gzip_extra_fields::Vector{GzipExtraField}

    # Read from this index in the current block
    read_index::UInt32

    # Number of bytes filled in buffer field
    buffer_filled::UInt32

    # Starting index of buffer
    buffer_start::UInt32

    # Current block to read from
    reading_block_index::UInt32

    # Block to write data to next. Must either point to the next empty block after
    # reading block index, or, if no blocks are empty, == reading_block_index
    writing_block_index::UInt32

    # If `reader.io` has been seen to be eof, we set this.
    underlying_is_eof::Bool

    check_truncated::Bool

    state::UInt8

    error::BGZFError

    # TODO: Add the number of consumed bytes to report position
    # position must be a virtual offset EXCLUSIVELY
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
    buffer = Memory{UInt8}(undef, (4 * MAX_BLOCK_SIZE) % Int)
    if DEBUG
        fill!(buffer, 0xaa)
    end
    return BGZFReader{typeof(io)}(
        io,
        blocks,
        buffer,
        sizehint!(GzipExtraField[], 4),
        1,
        0,
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
    reader.blocks = Memory{Blocks.Block{Decompressor}}()
    return close(reader.io)
end

function Base.eof(x::BGZFReader)
    x.state == STATE_CLOSED && return true
    isempty(BufIO.get_buffer(x)) || return false
    return iszero(BufIO.fill_buffer(x))
end

Base.isopen(reader::BGZFReader) = reader.state != STATE_CLOSED

# TODO: Implement isopen

Base.iswritable(::BGZFReader) = false

# TODO: Seek
# TODO: Position

function BufIO.get_buffer(reader::BGZFReader)
    reader.state == STATE_CLOSED && return @inbounds ImmutableMemoryView(reader.buffer)[1:0]
    block = reader.blocks[reader.reading_block_index]
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
# first unfilled block if eof, else reading_block_index.
# Return whether any block was queued
function queue!(reader::BGZFReader)::Bool
    buffer = reader.buffer
    blocks = reader.blocks

    # We queue in a loop, and when we hit EOF we need to check if we
    # have queued in a previous loop when exiting
    queued_any = false

    while true
        reader.buffer_filled < MAX_BLOCK_SIZE && fill_internal_buffer!(reader)

        @assert reader.buffer_filled ≥ MAX_BLOCK_SIZE || reader.underlying_is_eof

        iszero(reader.buffer_filled) && return queued_any

        (header_len, block_size) = get_bsize(reader)

        filled_view = ImmutableMemoryView(buffer)[
            reader.buffer_start:(reader.buffer_start + reader.buffer_filled - 1),
        ]

        # Block is the header, then compressed payload, then two UInt32
        # of the CRC32, and the decompressed length.
        compressed_payload_len = block_size - header_len % UInt32 - UInt32(8)
        target_crc32 = unsafe_bitload(UInt32, filled_view, block_size - 7)
        decompressed_payload_len = unsafe_bitload(UInt32, filled_view, block_size - 3)

        # No point in queueing empty blocks.
        if iszero(decompressed_payload_len)
            reader.buffer_start += block_size
            reader.buffer_filled -= block_size
            continue
        end

        # Queue the data in the block
        block = blocks[reader.writing_block_index]

        # We only queue idle blocks.
        state = @atomic :acquire block.state
        if !queued_any
            # If this is the first block to queue, the block must be idle.
            # I.e. we never call `queue!` when there can be zero blocks to queue.
            @assert state == STATE_IDLE
        end
        state == STATE_IDLE || return queued_any

        block.crc32 = target_crc32
        block.out_len = decompressed_payload_len
        block.in_len = compressed_payload_len
        copyto!(
            block.in_data,
            1,
            filled_view,
            header_len + 1,
            compressed_payload_len
        )
        # TODO: handle errors properly here, don't let it buble up
        Blocks.queue!(block)
        queued_any = true
        reader.buffer_start += block_size
        reader.buffer_filled -= block_size

        # Now, move on to next block to queue. If no other blocks, exit early
        length(blocks) == 1 && return true

        # Move to next block to queue
        reader.writing_block_index = next_block_index(reader, Int(reader.writing_block_index))
    end
    return
end

function fill_internal_buffer!(reader::BGZFReader)::Nothing
    buffer = reader.buffer

    # If we don't have enough room for a block, we copy around data in the buffer
    # to make room.
    unfilled = MemoryView(buffer)[(reader.buffer_filled + reader.buffer_start):lastindex(buffer)]
    if length(unfilled) < MAX_BLOCK_SIZE
        copyto!(buffer, 1, buffer, reader.buffer_start, reader.buffer_filled)
        reader.buffer_start = 1
        unfilled = MemoryView(buffer)[(reader.buffer_filled + 1):lastindex(buffer)]
    end
    @assert length(unfilled) >= MAX_BLOCK_SIZE

    while reader.buffer_filled < MAX_BLOCK_SIZE
        n_read = readbytes!(reader.io, unfilled)
        reader.buffer_filled += n_read

        # This happens if underlying is EOF
        if iszero(n_read)
            # Check for EOF block, only if we haven't checked before.
            if !reader.underlying_is_eof && reader.check_truncated
                filled = ImmutableMemoryView(buffer)[reader.buffer_start:(reader.buffer_start + reader.buffer_filled - 1)]
                if length(filled) < length(EOF_BLOCK) || filled[(end - length(EOF_BLOCK) + 1):end] != EOF_BLOCK
                    reader.state = STATE_ERROR
                    error!(reader, BGZFError(BGZFErrors.TruncatedFile))
                end
                reader.buffer_filled -= length(EOF_BLOCK)
                reader.underlying_is_eof = true
            end
            return nothing
        else
            reader.underlying_is_eof = false
        end
    end

    return nothing
end

function get_bsize(reader::BGZFReader)
    buffer = reader.buffer

    filled_view = ImmutableMemoryView(buffer)[
        reader.buffer_start:(reader.buffer_start + reader.buffer_filled - 1),
    ]

    # Parse the header of the gzip block to get the block size
    extra_fields = reader.gzip_extra_fields
    GC.@preserve filled_view begin
        mem = ReadableMemory(pointer(filled_view), length(filled_view) % UInt)
        parsed_header = parse_gzip_header(mem; extra_data = extra_fields)
    end

    if parsed_header isa LibDeflateError
        error!(reader, BGZFError(parsed_header))
    end
    header_len, _ = parsed_header
    bsize = get_bsize(extra_fields, filled_view)
    if bsize === nothing
        error!(reader, BGZFError(BGZFErrors.MissingBCField))
    end
    # By spec, BSIZE is block size -1. Include header_len bytes header, 8 byte tail
    block_size = bsize % UInt32 + UInt32(1)
    if block_size > reader.buffer_filled
        error!(reader, BGZFError(BGZFErrors.TooLargeBlockSize))
    end

    return (header_len, block_size)
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
    # TODO: Have a distinct queue function for sync readers, then have a distinct
    # fillbuffer function to call here for sync readers?
    if length(blocks) == 1
        # If the block couldn't be queued, it must be because of EOF
        queue!(reader) || return 0
        state = wait(block.task)
        state == STATE_ERROR && error!(reader, BGZFError(block.error))
        return block.out_len % Int
    end

    # If the underlying IO is EOF, we defer queuing until we run out of
    # buffered data. This way, the underlying stream has a chance to
    # un-EOF, if it has that option.
    was_eof_at_first = reader.underlying_is_eof
    was_eof_at_first || queue!(reader)

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
        queue!(reader) || return 0
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
