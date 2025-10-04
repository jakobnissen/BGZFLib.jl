module BGZFLib

# TODO: Tests.
# TODO: Writers

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
    BufReader,
    IOError,
    IOErrorKinds,
    get_buffer,
    get_nonempty_buffer,
    fill_buffer,
    consume

export BGZFReader, SyncBGZFReader, BGZFErrors, BGZFError, VirtualOffset, virtual_seek, virtual_position
public BGZFErrorType

const MAX_BLOCK_SIZE = 2^16
const DUMMY_BUFFER = Memory{UInt8}()

module BGZFErrors
    @enum BGZFErrorType::UInt8 begin
        truncated_file
        missing_bc_field
        inblock_offset_out_of_bounds
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
    block_offset::Union{Nothing, Int}
    type::Union{BGZFErrorType, LibDeflateError}
end

const BitInteger = Union{UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64}

# By specs, BGZF files are always little-endian
function unsafe_bitload(T::Type{<:BitInteger}, data::ImmutableMemoryView{UInt8}, p::Integer)
    return GC.@preserve data ltoh(unsafe_load(Ptr{T}(pointer(data, p))))
end

# By specs, BGZF files are always little-endian
function unsafe_bitstore!(v::BitInteger, data::MutableMemoryView{UInt8}, p::Integer)
    return GC.@preserve data unsafe_store!(Ptr{typeof(v)}(pointer(data, p)), htol(v))
end

"""
VirtualOffset(file_offset::Integer, block_offset::Integer)

Create a BGZF virtual file offset from `file_offset` and `block_offset`.
Get the two offsets with the public properties `vo.file_offset` and `vo.block_offset`

A `VirtualOffset` contains the two zero-indexed offset: The "file offset",
which is the offset in the *compressed* BGZF file that marks the beginning of
the block with the given position, and an "block offset" which is the
offset of the *uncompressed* content of that block.

The valid ranges of these two are `0:2^48-1` and `0:2^16-1`, respectively.
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

function Base.show(io::IO, x::VirtualOffset)
    return print(io, summary(x), '(', x.file_offset, ", ", x.block_offset, ')')
end

const STATE_OPEN = 0x00
const STATE_CLOSED = 0x01
const STATE_ERROR = 0x02

include("syncreader.jl")
include("reader.jl")

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
        buffer = get_nonempty_buffer(underlying)
        if isnothing(buffer)
            if last_was_empty === false
                return (; consumed, result = BGZFError(consumed, BGZFErrors.truncated_file))
            end
            return (; consumed, result = nothing)
        end

        while length(buffer) < MAX_BLOCK_SIZE
            increased = fill_buffer(underlying)
            if isnothing(increased)
                error(
                    "BGZF reader's underlying IO has an ungrowable buffer with a size " *
                        "smaller than 2^16 bytes. BGZF readers are only usable with `AbstractBufReaders` " *
                        "with buffers at least 2^16 bytes long."
                )
            end
            iszero(increased) && break
            buffer = get_buffer(underlying)
        end

        # Read all the extra data, where the BC field can be found
        # Header is 12 bytes
        length(buffer) < 12 && return (; consumed, result = BGZFError(consumed, BGZFErrors.truncated_file))
        ex_len = (@inbounds buffer[11] % Int) | ((@inbounds buffer[12] % Int) << 8)
        if length(buffer) < 12 + ex_len
            return (; consumed, result = BGZFError(consumed, BGZFErrors.truncated_file))
        end

        # Parse and validate the entire gzip header
        GC.@preserve buffer begin
            parsed_header = unsafe_parse_gzip_header(pointer(buffer), (12 + ex_len) % UInt, gzip_extra_fields)
        end
        parsed_header isa LibDeflateError && return (; consumed, result = BGZFError(consumed, parsed_header))

        # Read BC field, which gives the block size minus 1.
        fieldnum = findfirst(gzip_extra_fields) do field
            field.tag === (UInt8('B'), UInt8('C'))
        end
        fieldnum === nothing && return (; consumed, result = BGZFError(consumed, BGZFErrors.missing_bc_field))
        field = @inbounds gzip_extra_fields[fieldnum]
        if field.data === nothing || length(field.data) != 2
            return (; consumed, result = BGZFError(consumed, BGZFErrors.missing_bc_field))
        end
        block_size = ((buffer[first(field.data)] % Int) | ((buffer[last(field.data)] % Int) << 8)) + 1
        if length(buffer) < block_size
            return (; consumed, result = BGZFError(consumed, BGZFErrors.truncated_file))
        end
        # Minimal block size: 12 header bytes, 8 trailing bytes plus ex_len
        if block_size < ex_len + 20
            return (; consumed, result = BGZFError(consumed, BGZFErrors.truncated_file))
        end

        # Header is 12 bytes, extra fields is ex_len, 8 for CRC and decompressed size
        payload_span = (12 + ex_len + 1):(block_size - 8)
        payload = ImmutableMemoryView(buffer)[payload_span]
        decompressed_len = unsafe_bitload(UInt32, buffer, block_size - 3)
        expected_crc32 = unsafe_bitload(UInt32, buffer, block_size - 7)
        # Skip empty blocks, no need to decompress them
        if iszero(decompressed_len)
            consumed += block_size
            offset_at_block_start += block_size
            consume(underlying, block_size)
            if last_was_empty === false
                last_was_empty = true
            end
            continue
        end
        return (; consumed, result = (; payload, block_size = block_size % UInt32, decompressed_len, expected_crc32))
    end
    return
end

end # module BGZFLib
