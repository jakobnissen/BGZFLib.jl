module BGZFLib

const DEBUG = true

using MemoryViews: ImmutableMemoryView, MemoryView
using LibDeflate: ReadableMemory,
    Compressor,
    Decompressor,
    GzipExtraField,
    parse_gzip_header,
    LibDeflateError

export VirtualOffset, BGZFReader, BGZFError, consume, getbuffer, fillbuffer

# BGZF blocks are no larger than 64 KiB before and after compression.
const MAX_BLOCK_SIZE = UInt(64 * 1024)

"""
    BGZFError(message::String)

BGZF de/compressor errored with `message` when processing data."
"""
struct BGZFError <: Exception
    message::String
end

@noinline bgzferror(s::String) = throw(BGZFError(s))

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
include("virtualoffset.jl")

mutable struct BGZFReader{I <: IO}
    const io::I
    const blocks::Memory{Blocks.Block{Decompressor}}

    # We read from `io` in here
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

    # TODO: Add the number of consumed bytes to report position
    # position must be a virtual offset EXCLUSIVELY

    # TODO: Add an atomic field set when the reader errors, and check it in
    # queue!? Maybe like open, corrupted, closed
end

next_block_index(r::BGZFReader, i::Int) = i ≥ length(r.blocks) ? 1 : i + 1

function BGZFReader(
        io::IO;
        threads::Int = threads.nthreads(),
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
        # Start at last block, since that emulates having just exhausted an
        # empty block, then we start from 1.
        length(blocks),
        1,
        false,
        check_truncated,
    )
end

function Base.close(reader::BGZFReader)
    # TODO: switch atomic state once we get that to closed
    return close(reader.io)
end

# TODO: Implement isopen

Base.iswritable(::BGZFReader) = false

# TODO: Seek
# TODO: Position

function getbuffer(reader::BGZFReader)
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
    queued_any = false
    last_block_was_empty = false
    first_queued_block = 0

    while true
        # We can't know how much data the next block is, so make sure we have at least
        # a full block, unless we reach EOF.
        if reader.buffer_filled < MAX_BLOCK_SIZE
            unfilled = MemoryView(buffer)[(reader.buffer_filled + reader.buffer_start):lastindex(buffer)]
            @assert !iszero(length(unfilled))
            # If we don't have enough room for a block, we copy around data in the buffer
            # to make room.
            if length(unfilled) < MAX_BLOCK_SIZE
                copyto!(buffer, 1, buffer, reader.buffer_start, reader.buffer_filled)
                reader.buffer_start = 1
                unfilled = MemoryView(buffer)[(reader.buffer_filled + 1):lastindex(buffer)]
            end
            n_read = readbytes!(reader.io, unfilled)
            reader.buffer_filled += n_read
            if iszero(n_read)
                if reader.check_truncated
                    # TODO: proper error handling
                    error("Truncated file")
                end
                reader.underlying_is_eof = true
                # We might have queued a block in a previous iteration
                iszero(reader.buffer_filled) && return queued_any
            end
        end
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
            # TODO: Handle errors properly
            bgzferror("Block does not contain a valid gzip header")
        end
        header_len, _ = parsed_header
        bsize = get_bsize(extra_fields, filled_view)
        if bsize === nothing
            # TODO: Handle errors properly
            bgzferror("No GZIP extra field \"BSIZE\"")
        end
        # By spec, BSIZE is block size -1. Include header_len bytes header, 8 byte tail
        block_size = bsize % UInt32 + UInt32(1)
        if block_size > reader.buffer_filled
            # TODO: Handle errors properly
            bgzferror("BGZF block size points outside input file")
        end
        # Block is the header, then compressed payload, then two UInt32
        # of the CRC32, and the decompressed length.
        compressed_payload_len = block_size - header_len % UInt32 - UInt32(8)
        target_crc32 = unsafe_bitload(UInt32, filled_view, block_size - 7)
        decompressed_payload_len = unsafe_bitload(UInt32, filled_view, block_size - 3)

        # No point in queueing empty blocks.
        last_block_was_empty = iszero(decompressed_payload_len)
        if last_block_was_empty
            reader.buffer_start += block_size
            reader.buffer_filled -= block_size
            continue
        end

        # Queue the data in the block
        blocks = reader.blocks
        if first_queued_block == 0
            first_queued_block = reader.writing_block_index
        end
        println("Writing to $(reader.writing_block_index)")
        block = blocks[reader.writing_block_index]
        block.crc32 = target_crc32
        block.out_len = decompressed_payload_len
        block.in_len = compressed_payload_len
        # TODO: Use memcpy here
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

        # We stop queueing once we either:
        # A) Reach the first block again which we already queued, or
        # B) Reach a block that already have decompressed content in it
        reader.writing_block_index = next_block_index(reader, Int(reader.writing_block_index))
        if reader.writing_block_index == first_queued_block
            return true
        end
        block = blocks[reader.writing_block_index]
        if !iszero(block.out_len)
            if DEBUG
                @assert reader.writing_block_index == reader.reading_block_index
            end
            return true
        end
    end
    return
end

function fillbuffer(reader::BGZFReader)::Int
    # Check current block is empty, since BGZFReaders cannot expand an existing
    # buffer.
    blocks = reader.blocks
    reading_block_index = reader.reading_block_index % Int
    block = @inbounds blocks[reading_block_index]
    if reader.read_index ≤ block.out_len
        error("Can only call `fillbuffer` on a `BGZFReader` if the buffer is currently empty")
    end
    block.out_len = 0

    # If there's only one block, it's simple: Queue the block and check if it
    # has data
    if length(blocks) == 1
        queue!(reader) || return 0
        wait(block.task)
        reader.read_index = 1
        return block.out_len % Int
    else
        # If the underlying IO is EOF, we defer queuing until we run out of
        # buffered data. This way, the underlying stream has a chance to
        # un-EOF, if it has that option.
        queued = !reader.underlying_is_eof
        queued && queue!(reader)

        # Move to next block
        next_index = next_block_index(reader, reading_block_index)
        block = @inbounds blocks[next_index]

        # If it doesn't have any data, it could be because we didn't queue,
        # as reader was EOF - in that case, re-queue.
        # Or, it's legitimately empty, in which case, return zero
        if iszero(block.out_len)
            queued && return 0
            queue!(reader) || return 0
        end

        # We just checked it has data, so here, we just return it
        wait(block.task)

        out_len = block.out_len
        if DEBUG
            @assert !iszero(out_len)
        end
        reader.read_index = 1
        reader.reading_block_index = next_index % UInt32
        return out_len
    end
end

function consume(reader::BGZFReader, n::UInt)
    buf = getbuffer(reader)
    length(buf) % UInt < n && error("Consumed too much")
    reader.read_index = (reader.read_index + n) % UInt32
    return nothing
end

end # module BGZFLib
