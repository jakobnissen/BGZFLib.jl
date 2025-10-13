module BGZFLib

# TODO: Tests.

using MemoryViews: MemoryView, ImmutableMemoryView, MutableMemoryView
using LibDeflate: Compressor,
    Decompressor,
    GzipExtraField,
    unsafe_parse_gzip_header,
    unsafe_decompress!,
    unsafe_compress!,
    unsafe_crc32,
    LibDeflateError,
    LibDeflateErrors

using BufferIO: BufferIO,
    AbstractBufReader,
    AbstractBufWriter,
    BufReader,
    BufWriter,
    IOError,
    IOErrorKinds,
    get_buffer,
    get_nonempty_buffer,
    fill_buffer,
    grow_buffer,
    shallow_flush,
    get_unflushed,
    consume

export BGZFReader,
    BGZFWriter,
    SyncBGZFReader,
    SyncBGZFWriter,
    BGZFErrors,
    BGZFError,
    GZIndex,
    VirtualOffset,
    virtual_seek,
    virtual_position,
    write_empty_block,
    load_gzi,
    write_gzi,
    index_bgzf

public BGZFErrorType

@noinline unreachable()::Union{} = error("Unreachable statement reached, please file a bug report")

const MAX_BLOCK_SIZE = 2^16

# Compressing random data makes it larger, so only compress this many bytes.
const SAFE_MARGIN = 256
const SAFE_DECOMPRESSED_SIZE = MAX_BLOCK_SIZE - SAFE_MARGIN
const DUMMY_BUFFER = Memory{UInt8}()

"""
    module BGZFErrors

This module is used as a namespace for the enum `BGZFErrorType`.
The enum is non-exhaustive (more variants may be added in the future).
The current values are:

* `truncated_file`: The reader data stops abruptly. Either in the middle of a block,
  or there is no empty block at EOF
* `missing_bc_field`: A block has no `BC` field, or it's malformed
* `block_offset_out_of_bounds`: Seek with a `VirtualOffset` where the block offset
  is larger than the block size
* `insufficient_reader_space`: The BGZF reader wraps an `AbstractBufWriter` that is
  not EOF, and its buffer can't grow to encompass a whole BGZF block
* `insufficient_writer_space`: A BGZF writer wraps an `AbstractBufWriter` whose buffer
  cannot grow to encompass a full BGZF block
* `unsorted_index`: Attempted to load a malformed GZI file with unsorted coordinates
* `operation_on_error`: Attempted an operation on a BGZF reader or writer in an
  error state.
"""
module BGZFErrors
    @enum BGZFErrorType::UInt8 begin
        truncated_file
        missing_bc_field
        block_offset_out_of_bounds
        insufficient_reader_space
        insufficient_writer_space
        unsorted_index
        operation_on_error
    end

    export BGZFErrorType
end # module BGZFErrors

using .BGZFErrors

"""
    BGZFError <: Exception

Exception type thrown by BGZF readers and writers, when encountering errors specific to
the BGZF (or gzip, or DEFLATE) formats.
Note that exceptions thrown by BGZF readers and writers are not guaranteed to be of this type,
as they may also throw `BufferIO.IOError`s, or exceptions propagated by their underlying IO.

This error contains two public properties:
* `block_offset::Union{Nothing, Int}` gives the zero-based offset in the compressed stream
   of the block where the error occurred.
   Some errors may not occur at a specific block, in which case this is `nothing`.
* `type::Union{BGZFErrorType, LibDeflateError}`. If the blocks are malformed gzip blocks, this
   is a `LibDeflateError`. Else, if the error is specific to the BGZF format, it's a BGZFErrorType.
"""
struct BGZFError <: Exception
    # Block occurred in the block with this file offset (zero-based),
    # except for some errors which are not block-specific
    file_offset::Union{Nothing, Int}
    type::Union{BGZFErrorType, LibDeflateError}
end

# TODO: Show method. Give hint if operation_on_error

const BitInteger = Union{UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64}

# By specs, BGZF files are always little-endian
function unsafe_bitload(T::Type{<:BitInteger}, data::ImmutableMemoryView{UInt8}, p::Integer)
    return GC.@preserve data ltoh(unsafe_load(Ptr{T}(pointer(data, p))))
end

# By specs, BGZF files are always little-endian
function unsafe_bitstore!(v::BitInteger, data::MutableMemoryView{UInt8}, p::Integer)
    return GC.@preserve data unsafe_store!(Ptr{typeof(v)}(pointer(data, p)), htol(v))
end

const BLOCK_HEADER = [
    0x1f, 0x8b, # Magic bytes
    0x08, # Compression method is DEFLATE
    0x04, # Flags: Contains extra fields
    0x00, 0x00, 0x00, 0x00, # Modification time (mtime): Zero'd out
    0x00, # Extra flags: None used
    0xff, # Operating system: Unknown (we don't care about OS)
    0x06, 0x00, # 6 bytes of extra data to follow
    0x42, 0x43, # Xtra info tag: "BC"
    0x02, 0x00, # 2 bytes of data for tag "BC",
]

const EOF_BLOCK = vcat(
    BLOCK_HEADER,
    [
        0x1b, 0x00, # Total size of block - 1
        0x03, 0x00, # DEFLATE compressed load of the empty input
        0x00, 0x00, 0x00, 0x00, # CRC32 of the empty input
        0x00, 0x00, 0x00, 0x00,  # Input size of the empty input
    ]
)

"""
    VirtualOffset(file_offset::Integer, block_offset::Integer)

Create a BGZF virtual file offset from `file_offset` and `block_offset`.
Get the two offsets with the public properties `vo.file_offset` and `vo.block_offset`

A `VirtualOffset` contains the two zero-indexed offset: The "file offset",
which is the offset in the *compressed* BGZF file that marks the beginning of
the block with the given position, and an "block offset" which is the
offset of the *uncompressed* content of that block.

The valid ranges of these two are `0:2^48-1` and `0:2^16-1`, respectively.

# Examples
```jldoctest
julia> reader = SyncBGZFReader(CursorReader(bgzf_data));

julia> vo = VirtualOffset(178, 5)
VirtualOffset(178, 5)

julia> virtual_seek(reader, vo);

julia> String(read(reader, 9))
"some more"

julia> virtual_seek(reader, VirtualOffset(0, 7));

julia> String(read(reader, 6))
"world!"
```
"""
struct VirtualOffset
    x::UInt64

    function VirtualOffset(file_offset::Integer, block_offset::Integer)
        file_offset = UInt64(file_offset)::UInt64
        block_offset = UInt64(block_offset)::UInt64
        if file_offset ≥ 2^48
            throw(ArgumentError("block file offset must be in 0:281474976710655"))
        end
        if block_offset ≥ 2^16
            throw(ArgumentError("in-block offset must be in 0:65535"))
        end
        return new((UInt64(file_offset) << 16) | UInt64(block_offset))
    end
end

Base.propertynames(::VirtualOffset) = (:file_offset, :block_offset)

function Base.getproperty(vo::VirtualOffset, s::Symbol)
    return if s === :file_offset
        getfield(vo, :x) >>> 16
    elseif s === :block_offset
        getfield(vo, :x) % UInt16
    else
        getfield(vo, s)
    end
end

Base.:(<)(x::VirtualOffset, y::VirtualOffset) = getfield(x, :x) < getfield(y, :x)
Base.cmp(x::VirtualOffset, y::VirtualOffset) = cmp(getfield(x, :x), getfield(y, :x))

function Base.show(io::IO, x::VirtualOffset)
    return print(io, summary(x), '(', x.file_offset, ", ", x.block_offset, ')')
end

const STATE_OPEN = 0x00
const STATE_CLOSED = 0x01
const STATE_ERROR = 0x02

# Ensure that the sink of the BGZF writers can present a buffer of at least MAX_BLOCK_SIZE size.
# We run this in the constructor to precallocate. This also makes the constructor error early.
function get_writer_sink_room(io::AbstractBufWriter)::MutableMemoryView{UInt8}
    buffer = get_buffer(io)
    while length(buffer) < MAX_BLOCK_SIZE
        n = grow_buffer(io)
        iszero(n) && throw(BGZFError(nothing, BGZFErrors.insufficient_writer_space))
        buffer = get_buffer(io)
    end
    return buffer
end

function get_reader_source_room(io::AbstractBufReader)::Union{Nothing, ImmutableMemoryView{UInt8}}
    buffer = get_buffer(io)
    while length(buffer) < MAX_BLOCK_SIZE
        increased = fill_buffer(io)
        if isnothing(increased)
            throw(BGZFError(nothing, BGZFErrors.insufficient_reader_space))
        end
        if iszero(increased)
            return isempty(buffer) ? nothing : buffer
        end
        buffer = get_buffer(io)
    end
    return buffer
end

include("syncreader.jl")
include("syncwriter.jl")
include("reader.jl")
include("writer.jl")
include("index.jl")

function get_reader_block_work(
        underlying::AbstractBufReader,
        gzip_extra_fields::Vector{GzipExtraField},
        # Nothing means: Don't check
        last_was_empty::Union{Bool, Nothing},
        offset_at_block_start::Int,
    )::@NamedTuple{
        consumed::Int,
        result::Union{
            BGZFError,
            Nothing,
            @NamedTuple{
                payload::ImmutableMemoryView{UInt8},
                block_size::UInt32,
                decompressed_len::UInt32,
                expected_crc32::UInt32,
            },
        }
    }
    # Loop while we read empty blocks
    consumed = 0
    while true
        buffer = get_reader_source_room(underlying)
        if isnothing(buffer)
            if last_was_empty === false
                return (; consumed, result = BGZFError(consumed, BGZFErrors.truncated_file))
            end
            return (; consumed, result = nothing)
        end

        parsed = parse_bgzf_block!(gzip_extra_fields, buffer)
        if parsed isa LibDeflateError
            return (; consumed, result = BGZFError(consumed, parsed))
        elseif parsed isa BGZFErrorType
            return (; consumed, result = BGZFError(consumed, parsed))
        else
            if iszero(parsed.decompressed_len)
                consumed += parsed.block_size
                offset_at_block_start += parsed.block_size
                consume(underlying, Int(parsed.block_size))
                if last_was_empty === false
                    last_was_empty = true
                end
                continue
            end
            return (; consumed, result = parsed)
        end
    end
    return
end

function parse_bgzf_block!(
        gzip_extra_fields::Vector{GzipExtraField},
        buffer::ImmutableMemoryView{UInt8},
    )::Union{
        BGZFErrorType,
        LibDeflateError,
        @NamedTuple{
            payload::ImmutableMemoryView{UInt8},
            block_size::UInt32,
            decompressed_len::UInt32,
            expected_crc32::UInt32,
        },
    }

    # Read all the extra data, where the BC field can be found
    # Header is 12 bytes
    length(buffer) < 12 && return BGZFErrors.truncated_file
    ex_len = (@inbounds buffer[11] % Int) | ((@inbounds buffer[12] % Int) << 8)
    length(buffer) < 12 + ex_len && BGZFErrors.truncated_file

    # Parse and validate the entire gzip header
    GC.@preserve buffer begin
        parsed_header = unsafe_parse_gzip_header(pointer(buffer), (12 + ex_len) % UInt, gzip_extra_fields)
    end
    parsed_header isa LibDeflateError && return parsed_header

    # Read BC field, which gives the block size minus 1.
    fieldnum = findfirst(gzip_extra_fields) do field
        field.tag === (UInt8('B'), UInt8('C'))
    end
    fieldnum === nothing && return BGZFErrors.missing_bc_field
    field = @inbounds gzip_extra_fields[fieldnum]
    (field.data === nothing || length(field.data) != 2) && return BGZFErrors.missing_bc_field
    block_size = ((buffer[first(field.data)] % Int) | ((buffer[last(field.data)] % Int) << 8)) + 1
    length(buffer) < block_size && return BGZFErrors.truncated_file

    # Minimal block size: 12 header bytes, 8 trailing bytes plus ex_len
    block_size < ex_len + 20 && return BGZFErrors.truncated_file

    # Header is 12 bytes, extra fields is ex_len, 8 for CRC and decompressed size
    payload_span = ((12 + ex_len + 1) % UInt32):((block_size - 8) % UInt32)
    payload = @inbounds buffer[payload_span]
    decompressed_len = unsafe_bitload(UInt32, buffer, block_size - 3)
    expected_crc32 = unsafe_bitload(UInt32, buffer, block_size - 7)
    return (; payload, block_size = block_size % UInt32, decompressed_len, expected_crc32)
end

function compress_block!(
        dst::MutableMemoryView{UInt8},
        src::ImmutableMemoryView{UInt8},
        compressor::Compressor,
    )::Int
    # Don't try to compress too large chunks, and we must have checked
    # that destination has enough space.
    @assert length(src) ≤ SAFE_DECOMPRESSED_SIZE
    @assert length(dst) ≥ MAX_BLOCK_SIZE
    GC.@preserve dst src begin
        # Note: This should never be able to error, so we typeassert here
        libdeflate_return = unsafe_compress!(
            compressor,
            pointer(dst) + 18,
            length(dst) - 18,
            pointer(src),
            length(src),
        )::Int
        crc32 = unsafe_crc32(pointer(src), length(src))
    end
    # Header is 12 bytes. 6 bytes for the BC field. 8 bytes for CRC and decompressed size
    block_size = 18 + 8 + libdeflate_return
    # Copy header over, including first 4 bytes of BC field
    copyto!(dst, ImmutableMemoryView(BLOCK_HEADER))
    # Copy BC value over. Note that, if length(src) ≤ SAFE_DECOMPRESSED_SIZE, block_size
    # should never exceed typemax(UInt16), so this conversion should not error.
    unsafe_bitstore!(UInt16(block_size - 1), dst, 17)
    # Copy CRC32 and decompressed length
    unsafe_bitstore!(crc32, dst, block_size - 7)
    unsafe_bitstore!(length(src) % UInt32, dst, block_size - 3)
    return block_size
end


end # module BGZFLib
