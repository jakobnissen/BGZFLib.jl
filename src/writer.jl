const WRITER_BLOCK_SIZE = 4 * MAX_BLOCK_SIZE

# Struct sent to writer workers
struct WriterWork
    work_index::Int
    uncompressed::ImmutableMemoryView{UInt8}
    destination::Memory{UInt8}
end

# Struct received from writer workers
struct WriterResult
    work_index::Int
    unused::Memory{UInt8}
    result::Union{ImmutableMemoryView{UInt8}, Tuple{BGZFError, Memory{UInt8}}}
end

mutable struct BGZFWriter{T <: AbstractBufWriter} <: AbstractBufWriter
    const io::T

    # We use a pool of buffers so we can quickly replace a used up buffer while
    # the workers compress previous buffers asyncronously.
    # All buffers are reused, and we don't allocate new ones during operation.
    const buffer_pool::Vector{Memory{UInt8}}

    # FIFO queue - we take results from the receiver and add it here. Then, we popfirst
    # all finished results and flush them to io.io.
    # nothing means result is still at worker.
    const result_queue::Vector{Union{Nothing, ImmutableMemoryView{UInt8}, BGZFError}}
    const sender::Channel{WriterWork}
    const receiver::Channel{WriterResult}
    const workers::Memory{Task}

    # Current buffer to write into.
    buffer::Memory{UInt8}

    # Bytes 1:consumed have already been written
    consumed::Int

    # Number of WriterWork put into the sender
    n_shipped::Int

    # Number of WriterResult received from receiver.
    n_received::Int

    # Number of elements popped from FIFO queue. We use this number
    # to determine which index to add in new results to the queue,
    # e.g. if R result have been removed, then result number N is at index N - R.
    n_removed::Int

    # Write EOF block when closing the reader?
    write_eof::Bool
    state::UInt8
end

function BGZFWriter(
        io::AbstractBufWriter;
        n_workers::Int = min(4, Threads.nthreads()),
        write_eof::Bool = true,
        compress_level::Int = 6,
    )
    in(compress_level, 1:12) || throw(ArgumentError("compress_level must be in 1:12"))
    n_workers < 1 && throw(ArgumentError("Must have at least one worker"))
    get_writer_sink_room(io)
    sender = Channel{WriterWork}(Inf)
    receiver = Channel{WriterResult}(Inf)
    pool = [Memory{UInt8}(undef, WRITER_BLOCK_SIZE) for _ in 1:(2 * n_workers + min(2, cld(n_workers, 2)))]
    workers = Memory{Task}(undef, n_workers)
    for i in 1:n_workers
        task = Threads.@spawn writer_worker_loop(sender, receiver, compress_level)
        workers[i] = task
    end
    return BGZFWriter{typeof(io)}(
        io,
        pool,
        Vector{Union{Nothing, ImmutableMemoryView{UInt8}, BGZFError}}(),
        sender,
        receiver,
        workers,
        pop!(pool),
        0,
        0,
        0,
        0,
        write_eof,
        STATE_OPEN,
    )
end

Base.isopen(io::BGZFWriter) = io.state != STATE_CLOSED

function BGZFWriter(
        io::IO;
        n_workers::Int = min(4, Threads.nthreads()),
        write_eof::Bool = true,
        compress_level::Int = 6,
    )
    in(compress_level, 1:12) || throw(ArgumentError("compress_level must be in 1:12"))
    n_workers < 1 && throw(ArgumentError("Must have at least one worker"))
    bufio = BufWriter(io, MAX_BLOCK_SIZE)
    return BGZFWriter(bufio; n_workers, write_eof)
end

"""
    write_empty_block(io::BGZFWriter)::Int

Perform a `shallow_flush`, then write an empty block.
Return the number of bytes flushed.
"""
function write_empty_block(io::BGZFWriter)
    n = _shallow_flush(io)(io)
    write(io.io, EOF_BLOCK)
    return n
end

BufferIO.get_buffer(io::BGZFWriter) = MemoryView(io.buffer)[(io.consumed + 1):end]

function BufferIO.consume(io::BGZFWriter, n::Int)
    @boundscheck if n % UInt > (length(io.buffer) - io.consumed) % UInt
        throw(IOError(IOErrorKinds.ConsumeBufferError))
    end
    io.consumed += n
    return nothing
end

function check_open(io::BGZFWriter)
    return isopen(io) || error("Operation on closed BGZFWriter") # TODO: Proper error
end

# Not the same as BufferIO.shallow_flush, because we don't return
# the number of flushed bytes. I guess we could.
function _shallow_flush(io::BGZFWriter)
    grow_buffer(io)
    while io.n_received < io.n_shipped
        take_result(io)
    end
    flush_next_result_queue(io)
    return @assert isempty(io.result_queue)
end

function Base.flush(io::BGZFWriter)
    _shallow_flush(io)
    flush(io.io)
    return nothing
end

function Base.close(io::BGZFWriter)
    flush(io)
    if io.write_eof
        write(io.io, EOF_BLOCK)
    end
    close(io.receiver)
    close(io.sender)
    close(io.io)
    io.state = STATE_CLOSED
    return nothing
end

function BufferIO.grow_buffer(io::BGZFWriter)
    check_open(io)

    iszero(io.consumed) && return 0

    while true
        # Take available work from receiver to free up buffers
        if !isempty(io.receiver)
            @lock io.receiver begin
                while !isempty(io.receiver)
                    take_result(io)
                end
            end
        end

        # Flush all ready work to underlying IO
        flush_next_result_queue(io)

        # We need two buffers: One to replace the one we send to compression,
        # and one to store the compressed data.
        if length(io.buffer_pool) < 2
            take_result(io)
            continue
        end

        compressed = ImmutableMemoryView(io.buffer)[1:io.consumed]
        io.n_shipped += 1
        work = WriterWork(io.n_shipped, compressed, pop!(io.buffer_pool))
        put!(io.sender, work)

        io.buffer = pop!(io.buffer_pool)
        old_consumed = io.consumed
        io.consumed = 0
        return old_consumed
    end
    return 0
end

function take_result(io::BGZFWriter)
    result = take!(io.receiver)
    io.n_received += 1
    push!(io.buffer_pool, result.unused)
    value = if result.result isa Tuple
        (error, buffer) = result.result
        push!(io.buffer_pool, buffer)
        error
    else
        result.result
    end
    index = result.work_index - io.n_removed
    for _ in (length(io.result_queue) + 1):index
        push!(io.result_queue, nothing)
    end
    if index < 1
        @show io.n_removed
        @show result.work_index
        @show io.n_shipped
    end
    @assert isnothing(io.result_queue[index])
    io.result_queue[index] = value
    return nothing
end

function flush_next_result_queue(io::BGZFWriter)
    while !isempty(io.result_queue) && !isnothing(first(io.result_queue))
        fst = popfirst!(io.result_queue)
        io.n_removed += 1
        if fst isa BGZFError
            io.state = STATE_ERROR
            throw(fst)
        else
            write(io.io, fst)
            push!(io.buffer_pool, parent(fst))
        end
    end
    return nothing
end

function writer_worker_loop(
        work_channel::Channel{WriterWork},
        result_channel::Channel{WriterResult},
        compress_level::Int,
    )
    compressor = Compressor(compress_level)
    for work in work_channel
        has_errored = false
        n_written = 0
        for src in Iterators.partition(work.uncompressed, SAFE_DECOMPRESSED_SIZE)
            dst = MemoryView(work.destination)[(n_written + 1):end]
            block_size = compress_block!(dst, src, compressor)
            yield()
            if block_size isa LibDeflateError
                error = BGZFError(nothing, block_size)
                result = WriterResult(
                    work.work_index,
                    parent(work.uncompressed),
                    (error, work.destination),
                )
                put!(result_channel, result)
                has_errored = true
                break
            end
            n_written += block_size
        end
        has_errored && continue
        result = WriterResult(
            work.work_index,
            parent(work.uncompressed),
            ImmutableMemoryView(work.destination)[1:n_written],
        )
        put!(result_channel, result)
    end
    return
end

function compress_block!(
        dst::MutableMemoryView{UInt8},
        src::ImmutableMemoryView{UInt8},
        compressor::Compressor,
    )::Union{LibDeflateError, Int}
    @assert length(src) ≤ SAFE_DECOMPRESSED_SIZE
    @assert length(dst) ≥ MAX_BLOCK_SIZE
    GC.@preserve dst src begin
        libdeflate_return = unsafe_compress!(
            compressor,
            pointer(dst) + 18,
            length(dst) - 18,
            pointer(src),
            length(src),
        )
        crc32 = unsafe_crc32(pointer(src), length(src))
    end
    libdeflate_return isa LibDeflateError && return libdeflate_return
    block_size = 18 + 8 + libdeflate_return
    copyto!(dst, ImmutableMemoryView(BLOCK_HEADER))
    unsafe_bitstore!(UInt16(block_size - 1), dst, 17)
    unsafe_bitstore!(crc32, dst, block_size - 7)
    unsafe_bitstore!(length(src) % UInt32, dst, block_size - 3)
    return block_size
end
