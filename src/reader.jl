const BLOCKS_PER_PACKAGE = 4
const BUFREADER_BUFFER_SIZE = 2 * MAX_BLOCK_SIZE

# The decompressor worker gets a vector of these, each representing one block
# to be decompressed
struct ReaderBlockWork
    source::ImmutableMemoryView{UInt8}
    destination::Memory{UInt8}
    file_offset::Int
    expected_crc32::UInt32
    decompressed_len::UInt32
end

# One package (i.e. workload) for a decompressor worker task.
struct ReaderWorkPackage
    # io.n_buffers_shipped_or_skipped when this package was constructed
    buffer_offset::Int
    block_works::Vector{ReaderBlockWork}
end

struct ReaderPackageResult
    # Copied from the corresponding ReaderWorkPackage
    buffer_offset::Int
    # (file_offset, decompressed_data), or an error for each block, in order
    results::Vector{Union{BGZFError, Tuple{Int, ImmutableMemoryView{UInt8}}}}
    # This contains all the memory backing the `ReaderBlockWork.source`, which
    # need to be recycled. If the `results` contain any errors, it also contains
    # the Memory backing the view which would otherwise be in the `results`
    buffers::Vector{Memory{UInt8}}
end

"""
    BGZFReader(io::T <: IO; n_workers::Int, check_truncated::Bool=true)::BGZFReader{BufReader{T}}
    BGZFReader(io::T <: AbstractBufReader; n_workers::Int, check_truncated::Bool=true)::BGZFReader{T}

Create a `BGZFReader <: AbstractBufReader` that decompresses a BGZF stream.

When constructing from an `io::AbstractBufReader`, `io` must have a buffer size of at least
$(MAX_BLOCK_SIZE), or be able to grow its buffer to this size.

If `check_truncated`, the last BGZF block in the file must be empty, otherwise the reader
throws an error. This can be used to detect the file was truncated.

The decompression happens asyncronously in a set of worker tasks. To avoid spawning workers,
use the `SyncBGZFReader` instead.

If the reader encounters an error, it goes into an error state and throws an exception.
The reader can be reset by using `seek` or `seekstart`. A closed reader cannot be reset.
"""
mutable struct BGZFReader{T <: AbstractBufReader} <: AbstractBufReader
    # This needs to have at least 2^16 bytes buffersize.
    const io::T

    # Cached so the main task does not have to allocate a new vector all the time
    const gzip_extra_fields::Vector{GzipExtraField}

    # Buffers can be: in the `io.buffer` field, in workers, in either of the two channels,
    # or in the result queue. All remaining buffers are stored here. They are all recycled,
    # so we never have to allocate new ones.
    const buffer_pool::Vector{Memory{UInt8}}

    # This is a FIFO queue. We take results from the receiver, and place it in this queue,
    # using the package index to order the results correctly.
    # `nothing` indicates that this package has not yet been received, i.e. if we receive
    # packages 1 and 3, then index 2 is nothing.
    # A non-error result is (file_offset of block, decompressed block)
    const result_queue::Vector{Union{Nothing, BGZFError, Tuple{Int, ImmutableMemoryView{UInt8}}}}
    const sender::Channel{ReaderWorkPackage}
    const receiver::Channel{ReaderPackageResult}
    const workers::Memory{Task}

    # Buffers popped off the result queue (using popfirst!) and go here.
    # After it is used up, its parent is recycled and goes to the buffer pool.
    # It's initialized with an empty memory which is discarded, not recycled
    buffer::Memory{UInt8}
    buffer_pos::Int
    buffer_filled::Int

    # This is essentially only used for `position(io)`. It gives the offset in the compressed file
    # of the current active buffer
    current_block_offset::Int

    # We keep track of the number of buffers we've shipped to workers, and how many we've
    # received from workers. This has three purposes:
    # First, each work package contains the buffer offset (i.e. buffers shipped before this one),
    # so that received work can be ordered correctly.
    # Second, we can check if there are any data in the workers or the channels by comparing
    # these two numbers. This tells us e.g. if we're really EOF, and whether we can safely wait
    # for more data from workers without deadlocking.
    # Third, it allows us to invalidate results from workers when skipping or seeking.
    # If we increment these counters and also `queue_n_removed` by a lot, then any data from workers
    # will have a negative index in the queue. This signals the data is invalid and can be discarded.
    n_buffers_shipped_or_skipped::Int
    n_buffers_received_or_skipped::Int

    # In order to allow us to discard data from the queue and still compute the correct ordering,
    # need to keep track of how many we `popfirst!`ed. I.e. if we get the Nth buffer, it should
    # go to index `N - queue_n_removed` in the result queue
    queue_n_removed_or_skipped::Int

    # Bytes read from the underlying IO. Each buffer processed by a worker must keep
    # track of the (zero based) file offset to report position.
    n_bytes_read::Int

    # Error if EOF is reached and last block was not empty. This is a BGZF feature.
    check_truncated::Bool
    last_was_empty::Bool
    state::UInt8 # open, closed or error
end

function BGZFReader(
        io::AbstractBufReader;
        n_workers::Int = min(4, Threads.nthreads()),
        check_truncated::Bool = true,
    )
    n_workers < 1 && throw(ArgumentError("Must have at least one worker"))
    get_reader_source_room(io)
    sender = Channel{ReaderWorkPackage}(Inf)
    receiver = Channel{ReaderPackageResult}(Inf)
    # Each worker chunk ('package') contains 1 input and 1 output buffer per block
    pool = [Memory{UInt8}(undef, MAX_BLOCK_SIZE) for _ in 1:total_buffers(n_workers)]
    workers = Memory{Task}(undef, n_workers)
    for i in 1:n_workers
        task = Threads.@spawn reader_worker_loop(sender, receiver)
        workers[i] = task
    end
    return BGZFReader{typeof(io)}(
        io,
        Vector{GzipExtraField}(),
        pool,
        Vector{Union{Nothing, BGZFError, Tuple{Int, ImmutableMemoryView{UInt8}}}}(),
        sender,
        receiver,
        workers,
        DUMMY_BUFFER,
        1, # pos
        0, # buffer filled
        0, # block offset
        0, # packages shipped
        0, # packages received
        0, # buffers consumed
        0, # bytes read
        check_truncated,
        false,
        STATE_OPEN,
    )
end

# I don't quite understand why, but adding some extra buffers makes the workers wait less and speeds
# up the code. It costs about 256 KiB memory per extra buffering, so we add it sparingly.
extra_buffering(n_workers::Int) = min(4, cld(n_workers, 2))

function total_buffers(n_workers::Int)
    return 2 * (n_workers + extra_buffering(n_workers)) * BLOCKS_PER_PACKAGE
end

function BGZFReader(
        io::IO;
        n_workers::Int = min(4, Threads.nthreads()),
        check_truncated::Bool = true
    )
    n_workers < 1 && throw(ArgumentError("Must have at least one worker"))
    bufio = BufReader(io, BUFREADER_BUFFER_SIZE)
    return BGZFReader(bufio; n_workers, check_truncated)
end

BufferIO.get_buffer(io::BGZFReader) = ImmutableMemoryView(io.buffer)[io.buffer_pos:io.buffer_filled]

function BufferIO.consume(io::BGZFReader, n::Int)
    @boundscheck if n % UInt > (io.buffer_filled - io.buffer_pos + 1) % UInt
        throw(IOError(IOErrorKinds.ConsumeBufferError))
    end
    io.buffer_pos += n
    return nothing
end

function Base.close(io::BGZFReader)
    # Close channels to finish the workers
    close(io.receiver)
    close(io.sender)
    empty!(io.sender)
    empty!(io.result_queue)
    empty!(io.buffer_pool)
    io.buffer = DUMMY_BUFFER
    close(io.io)
    io.state = STATE_CLOSED
    empty!(io.receiver)
    return nothing
end

Base.isopen(io::BGZFReader) = io.state != STATE_CLOSED


function throw_error(io::BGZFReader, err::BGZFError)
    io.buffer_pos = 1
    io.buffer_filled = 0
    io.state = STATE_ERROR
    throw(err)
end

"""
    seek(io::Union{BGZFReader, SyncBGZFReader}, offset::Int)

Seek to file offset `offset`. This is equivalent to seeking to `VirtualOffset(offset, 0)`.
"""
function Base.seek(io::BGZFReader, offset::Int)
    io.state == STATE_CLOSED && throw(IOError(IOErrorKinds.ClosedIO))

    seek(io.io, offset)
    io.n_bytes_read = offset
    io.current_block_offset = offset
    io.last_was_empty = false
    io.buffer_pos = 1
    io.buffer_filled = 0
    # Empty the queue. We ignore errors, and recycle any buffers
    for result in io.result_queue
        if result isa Tuple
            push!(io.buffer_pool, parent(result[2]))
        end
    end
    empty!(io.result_queue)

    # By incrementing these counters, all work currently in workers or channels
    # is invalidated and is ignored. That logic is in `take_package!`
    to_increment = total_buffers(length(io.workers))
    io.n_buffers_received_or_skipped += to_increment
    io.n_buffers_shipped_or_skipped += to_increment

    # Note: Since the next outgoing package has its offset determined by `n_buffers_shipped_or_skipped`,
    # we need to pretend we've removed these. Otherwise, the computed index will be too high in
    # the queue.
    io.queue_n_removed_or_skipped = io.n_buffers_shipped_or_skipped
    io.state = STATE_OPEN
    return io
end

function virtual_position(io::BGZFReader)
    return VirtualOffset(io.current_block_offset, io.buffer_pos - 1)
end

function virtual_seek(io::BGZFReader, vo::VirtualOffset)
    seek(io, vo.file_offset % Int)
    fill_buffer(io)
    if io.buffer_filled < vo.block_offset
        throw(BGZFError(vo.file_offset % Int, BGZFErrors.inblock_offset_out_of_bounds))
    end
    io.buffer_pos += vo.block_offset
    return io
end

function reader_worker_loop(
        packages::Channel{ReaderWorkPackage},
        results::Channel{ReaderPackageResult},
    )
    decompressor = Decompressor()
    for package in packages
        block_results = Vector{Union{BGZFError, Tuple{Int, ImmutableMemoryView{UInt8}}}}()
        buffers = Vector{Memory{UInt8}}()
        result = ReaderPackageResult(package.buffer_offset, block_results, buffers)
        for block_work in package.block_works
            source = block_work.source
            destination = block_work.destination
            GC.@preserve source destination begin
                libdeflate_return = unsafe_decompress!(
                    Base.HasLength(),
                    decompressor,
                    pointer(destination),
                    block_work.decompressed_len,
                    pointer(source),
                    length(source),
                )
            end
            yield()
            block_result = if libdeflate_return isa LibDeflateError
                BGZFError(block_work.file_offset, libdeflate_return)
            else
                GC.@preserve destination begin
                    crc32 = unsafe_crc32(pointer(destination), block_work.decompressed_len)
                end
                if crc32 != block_work.expected_crc32
                    BGZFError(block_work.file_offset, LibDeflateErrors.gzip_bad_crc32)
                else
                    (block_work.file_offset, ImmutableMemoryView(destination)[1:block_work.decompressed_len])
                end
            end
            push!(block_results, block_result)
            push!(buffers, parent(source))
            if block_result isa BGZFError
                push!(buffers, destination)
            end
        end
        put!(results, result)
    end
    return
end

function Base.eof(io::BGZFReader)
    # No data immediately available
    io.buffer_pos > io.buffer_filled || return false

    # No data waiting to be moved to the buffer
    isempty(io.result_queue) || return false

    # No data being processed in workers
    io.n_buffers_shipped_or_skipped == io.n_buffers_received_or_skipped || return false

    # No more data in underlying IO which can be decompressed
    return eof(io.io)::Bool
end

function BufferIO.fill_buffer(io::BGZFReader)
    io.state == STATE_CLOSED && return 0
    io.state == STATE_ERROR && error("Reader is in an error state. Use `seek` or `virtual_seek` to reset it")

    # Check if block has data already, we can't expand it. Return nothing.
    io.buffer_pos > io.buffer_filled || return nothing

    # Reuse buffer, except if it's the dummy buffer (which it is initialized with),
    # or if we previously tried to fill the buffer but couldnt.
    if !isempty(io.buffer)
        push!(io.buffer_pool, io.buffer)
        io.buffer = DUMMY_BUFFER
        io.buffer_filled = 0
        io.buffer_pos = 1
    end

    # Get results from workers, if any are ready. This allows us to 'move along'
    # and queue more stuff more eagerly
    if !isempty(io.receiver)
        lock(io.receiver) do
            while !isempty(io.receiver)
                take_package!(io)
            end
        end
    end

    # One package takes 2 * BLOCKS_PER_PACKAGE buffers. Queue all the packages we can,
    # i.e. until we run out of buffers or until the underlying io hits EOF
    if length(io.buffer_pool) ≥ 2 * BLOCKS_PER_PACKAGE
        queue!(io)
    end

    # If any buffers are immediately available in our result queue,
    # we move to that and return
    if !isempty(io.result_queue)
        n_filled = take_buffer_if_some!(io)
        n_filled isa Int && return n_filled
    end

    # Are there any workers active? Then we wait until the next buffer is available.
    seen_invalidated = false
    while io.n_buffers_received_or_skipped < io.n_buffers_shipped_or_skipped
        take_package!(io)

        # This can be empty if, in take_package! we ignored the previous package
        if isempty(io.result_queue)
            seen_invalidated = true
            continue
        end

        n_filled = take_buffer_if_some!(io)
        n_filled isa Int && return n_filled
    end

    # Handle the very unlikely case where all buffers were at the workers, and invalid.
    # If that happens, then no buffers can be queued, and no buffers can be used.
    # We just try this function again. This can't happen twice, because after the
    # loop above, we have all the buffers, and so can queue new work which cannot
    # be invalid.
    if seen_invalidated
        return fill_buffer(io)
    end

    # No data in buffer, no workers were active, even after queuing all workers
    # until EOF. If we reach this point, we are EOF.
    return 0
end

function queue!(io::BGZFReader)
    while length(io.buffer_pool) ≥ 2 * BLOCKS_PER_PACKAGE
        fst = get_reader_block_work(io)
        isnothing(fst) && return nothing
        block_works = ReaderBlockWork[fst]
        for _ in 1:(BLOCKS_PER_PACKAGE - 1)
            block = @something get_reader_block_work(io) break
            push!(block_works, block)
        end
        package = ReaderWorkPackage(io.n_buffers_shipped_or_skipped, block_works)
        io.n_buffers_shipped_or_skipped += length(block_works)
        @assert !isfull(io.sender)
        put!(io.sender, package)
    end
    return
end

# Take a package from the receiver (which we've verified is either not empty,
# or a worker is busy producing a value), and move the data to the result queue
function take_package!(io::BGZFReader)
    result = take!(io.receiver)
    io.n_buffers_received_or_skipped += length(result.results)
    append!(io.buffer_pool, result.buffers)
    index_offset = result.buffer_offset - io.queue_n_removed_or_skipped
    # When seeking, `io.queue_n_removed_or_skipped` is incremented by a lot. This will cause the
    # index offset of all work currently in the channels or workers to receive a negative index offset.
    # This indicates they are invalidated. We ignore them, but recycle the buffers
    if index_offset < 0
        for i in eachindex(result.results)
            res = result.results[i]
            if !isa(res, BGZFError)
                push!(io.buffer_pool, parent(res[2]))
            end
        end
        return nothing
    end
    nothings_to_add = (index_offset + length(result.results) - length(io.result_queue))
    for _ in 1:nothings_to_add
        push!(io.result_queue, nothing)
    end
    for i in eachindex(result.results)
        index = i + index_offset
        @assert isnothing(io.result_queue[index])
        io.result_queue[i + index_offset] = result.results[i]
    end
    return
end

# Take the next buffer or error from the result queue
function take_buffer_if_some!(io::BGZFReader)::Union{Nothing, Int}
    result = first(io.result_queue)
    return if result isa BGZFError
        throw_error(io, result)
    elseif result isa Tuple{Int, ImmutableMemoryView{UInt8}}
        (block_offset, buffer) = result
        io.current_block_offset = block_offset
        io.buffer = parent(buffer)
        io.buffer_filled = last(only(parentindices(buffer)))
        io.buffer_pos = 1
        popfirst!(io.result_queue)
        io.queue_n_removed_or_skipped += 1
        length(buffer)
    elseif result === nothing
        nothing
    else
        error() # unreachable
    end
end

# Read data from io.io to create work for a single block, or, if io.io is EOF,
# return nothing
function get_reader_block_work(io::BGZFReader)::Union{Nothing, ReaderBlockWork}
    last_was_empty = io.check_truncated ? io.last_was_empty : nothing
    (; consumed, result) = get_reader_block_work(io.io, io.gzip_extra_fields, last_was_empty, io.n_bytes_read)
    io.n_bytes_read += consumed
    if result === nothing
        io.last_was_empty = true
        return nothing
    elseif result isa BGZFError
        throw_error(io, result)
    else
        io.last_was_empty = false
        source = MemoryView(pop!(io.buffer_pool))[1:length(result.payload)]
        destination = pop!(io.buffer_pool)
        copy!(source, result.payload)
        consume(io.io, Int(result.block_size))
        file_offset = io.n_bytes_read + consumed
        io.n_bytes_read += result.block_size
        return ReaderBlockWork(ImmutableMemoryView(source), destination, file_offset, result.expected_crc32, result.decompressed_len)
    end
end
