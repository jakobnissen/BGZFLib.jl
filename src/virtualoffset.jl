"""
VirtualOffset(block_offset::Integer, inblock_offset::Integer)

Create a BGZF virtual file offset from `block_offset` and `inblock_offset`.
Get the two offsets with [`offsets(::VirtualOffset)`](@ref).

A `VirtualOffset` contains the two zero-indexed offset: The "block offset",
which is the offset in the *compressed* BGZF file that marks the beginning of
the block with the given position, and an "in-block offset" which is the
offset of the *uncompressed* content of that block.

The valid ranges of these two are `0:2^48-1` and `0:2^16-1`, respectively.
"""
struct VirtualOffset
    x::UInt64

    function VirtualOffset(block_offset::Integer, inblock_offset::Integer)
        block_offset = UInt64(block_offset)::UInt64
        inblock_offset = UInt64(inblock_offset)::UInt64
        if block_offset ≥ 2^48
            throw(ArgumentError("block file offset must be in 0:281474976710655"))
        end
        if inblock_offset ≥ 2^16
            throw(ArgumentError("in-block offset must be in 0:65535"))
        end
        return reinterpret(VirtualOffset, (UInt64(block_offset) << 16) | UInt64(inblock_offset))
    end
end

offsets(x::VirtualOffset) = (x.x >>> 16, x.x & 0xffff)

function Base.show(io::IO, x::VirtualOffset)
    v, o = offsets(x)
    return print(io, summary(x), '(', v, ", ", o, ')')
end
