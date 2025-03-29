"""
    BGZF blocks
 
Code in this file should not "know about" Codecs, TranscodingStreams,
or any of that. It should only rely on Base and LibDeflate, so that means the
code in this file can easily be cannibalized for other packages, or repurposed.
"""
module Blocks

# TODO: Update LibDeflate to handle memory views, to remove all pointer
# stuff from this package.

using Base.Threads: @spawn

using LibDeflate: Compressor,
    Decompressor,
    LibDeflateError,
    unsafe_decompress!,
    unsafe_crc32

using MemoryViews: ImmutableMemoryView

import ..bgzferror, ..unsafe_bitload, ..DE_COMPRESSOR, ..MAX_BLOCK_SIZE

export Block, SAFE_UNCOMPRESSED_BLOCK_SIZE

to_memory(x::Vector) = copy!(Memory{eltype(x)}(undef, length(x)), x)

# Every BGZF block begins with exactly these bytes.
# Note that we MUST e.g. ignore mtime and OS to be completely BGZF compliant
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
] |> to_memory

const EOF_BLOCK = vcat(
    BLOCK_HEADER,
    [
        0x1b, 0x00, # Total size of block - 1
        0x03, 0x00, # DEFLATE compressed load of the empty input
        0x00, 0x00, 0x00, 0x00, # CRC32 of the empty input
        0x00, 0x00, 0x00, 0x00,  # Input size of the empty input
    ]
)

# Maximum number of bytes to be compressed at one time. Random bytes usually end up filling
# a bit more when compressed, so we have a generous 256 byte margin of safety.
const SAFE_UNCOMPRESSED_BLOCK_SIZE = UInt(MAX_BLOCK_SIZE - 256)

# Field descriptions are for decompressors / compressors
mutable struct Block{T <: DE_COMPRESSOR}
    const de_compressor::T
    const out_data::Memory{UInt8}
    const in_data::Memory{UInt8}
    task::Task
    crc32::UInt32    # stated checksum / calculated checksum

    # BGZF blocks can store 0:typemax(UInt16)+1 bytes
    # so unfortunately UInt16 will not suffice here.
    out_len::UInt32   # Length of decompressed payload / compressed block
    in_len::UInt32    # Length of compressed payload / total input block
end

function Block(dc::T) where {T <: DE_COMPRESSOR}
    out_data = Memory{UInt8}(undef, MAX_BLOCK_SIZE % Int)
    in_data = similar(out_data)

    # We initialize with a trivial, but completable task for sake of simplicity
    task = schedule(Task(() -> nothing))
    return Block{T}(dc, out_data, in_data, task, 0, 0, 0)
end

function queue!(block::Block{Decompressor})
    return block.task = @spawn begin
        (in_data, out_data) = (block.in_data, block.out_data)
        GC.@preserve in_data out_data begin
            compress = unsafe_decompress!(
                Base.HasLength(),
                block.de_compressor,
                pointer(out_data), block.out_len,
                pointer(in_data), block.in_len
            )
            compress isa LibDeflateError && bgzferror("Invalid DEFLATE content in BGZF block")
            crc32 = unsafe_crc32(pointer(out_data), block.out_len)
        end
        crc32 != block.crc32 && bgzferror("CRC32 checksum does not match")
        return nothing
    end
end

end # module
