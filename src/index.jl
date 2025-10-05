const IndexBlock = @NamedTuple{compressed_offset::UInt64, decompressed_offset::UInt64}

# must be sorted, else BGZF error. EOFerror. must be little endian
"""
    GZIndex(blocks::Vector{@NamedTuple{compressed_offset::UInt64, decompressed_offset::UInt64}})

Construct a GZI index of a BGZF file. The vector `blocks` contains one pair of integers for
each block in the BGZF file, in order, containing the zero-based offset of the compressed
data and the corresponding decompressed data, respectively.

Throw a `BGZFError(nothing, BGZFErrors.unsorted_index)` if either of the offsets are not
sorted in ascending order.

Usually constructed with [`gzindex`](@ref), or [`load_index`](@ref)
and serialized with `write(io, ::GZIndex)`.

This struct contains the public property `.blocks` which corresponds to the vector
as described above, no matter how `GZIndex` is constructed.
"""
struct GZIndex
    blocks::Vector{IndexBlock}

    function GZIndex(v::Vector{IndexBlock})
        if !is_sorted(ImmutableMemoryView(v))
            throw(BGZFError(nothing, BGZFErrors.unsorted_index))
        end
        return new(v)
    end

    global function new_gzindex(v::Vector{IndexBlock})
        return new(v)
    end
end

Base.write(io::AbstractBufWriter, index::GZIndex) = write_gz(io, index)
Base.write(io::IO, index::GZIndex) = write_gz(io, index)

function write_gz(io::Union{AbstractBufWriter, IO}, index::GZIndex)
    blocks = index.blocks
    write(io, htol(length(blocks)))
    if htol(0x0102) != 0x0102
        error("This function assumes little-endian CPUs.")
    end
    GC.@preserve blocks begin
        p = Ptr{UInt8}(pointer(blocks))
        unsafe_write(io, p, sizeof(blocks) % UInt)
    end
    return 8 + 16 * length(blocks)
end

function get_buffer_with_length(io::AbstractBufReader, len::Int)::Union{Nothing, ImmutableMemoryView{UInt8}}
    buffer = get_buffer(io)
    while length(buffer) < len
        filled = fill_buffer(io)
        if isnothing(filled) || iszero(filled)
            return nothing
        end
        buffer = get_buffer(io)
    end
    return buffer
end

function is_sorted(blocks::ImmutableMemoryView{IndexBlock})
    isempty(blocks) && return true
    fst = @inbounds blocks[1]
    (co, dco) = (fst.compressed_offset, fst.decompressed_offset)
    # We don't return early because we want this function to SIMD,
    # and we expect that almost all GZIndices are sorted, so returning
    # early would inhibit SIMD for little gain.
    good = true
    for i in 2:lastindex(blocks)
        (; compressed_offset, decompressed_offset) = @inbounds blocks[i]
        good &= (co ≤ compressed_offset) & (dco ≤ decompressed_offset)
        co = compressed_offset
        dco = decompressed_offset
    end
    return good
end

"""
    load_index(io::Union{IO, AbstractBufReader})::GZIndex

Load a `GZIndex` from a GZI file.

Throw an `IOError(IOErrorKinds.EOF)` if `io` does not contain enough bytes for a valid
GZI file. Throw a `BGZFError(nothing, BGZFErrors.unsorted_index)` if the offsets are not
sorted in ascending order.
Currently does not throw an error if the file contains extra appended bytes, but this may
change in the future.
"""
load_index(io::IO) = load_index(BufReader(io))

function load_index(io::AbstractBufReader)
    # Load the length as a UInt64
    buffer = get_buffer_with_length(io, 8)
    buffer === nothing && throw(IOError(IOErrorKinds.EOF))
    len = unsafe_bitload(UInt64, buffer, 1)
    @inbounds consume(io, 8)
    # No way the file is 1 PiB in size, so this is reasonable
    len > 2^48 && throw(IOError(IOErrorKinds.EOF))
    len = len % Int
    blocks = Vector{IndexBlock}(undef, len)
    total_bytes = 16 * len
    # Julia guarantees the memory layout of bitstypes so this will work.
    GC.@preserve blocks begin
        n_read = unsafe_read(io, Ptr{UInt8}(pointer(blocks)), total_bytes % UInt)
    end
    n_read == total_bytes || throw(IOError(IOErrorKinds.EOF))
    return GZIndex(blocks)
end

"""
    gzindex(io::Union{IO, AbstractBufReader})::GZIndex

Compute a `GZIndex` from a BGZF file.

Throw a `BGZFError` if the BGZF file is invalid,
or a `BGZFError` with `BGZFErrors.insufficient_reader_space` if
an entire block cannot be buffered by `io`, (only happens if `io::AbstractBufReader`).
"""
gzindex(io::IO) = gzindex(BufReader(io))

function gzindex(io::AbstractBufReader)
    decompressed_offset = compressed_offset = 0
    blocks = IndexBlock[]
    gzip_fields = GzipExtraField[]
    while true
        buffer = get_reader_source_room(io)
        # Empty file is a valid BGZF file
        isnothing(buffer) && return new_gzindex(blocks)
        parsed = parse_bgzf_block!(gzip_fields, buffer)
        if parsed isa Union{BGZFErrorType, LibDeflateError}
            throw(BGZFError(compressed_offset, parsed))
        end
        (; block_size, decompressed_len) = parsed
        push!(blocks, (; compressed_offset, decompressed_offset))
        compressed_offset += block_size
        decompressed_offset += decompressed_len
        @inbounds consume(io, block_size % Int)
    end
    return
end
