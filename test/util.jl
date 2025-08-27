using LibDeflate
using BGZFLib.Blocks: BLOCK_HEADER, EOF_BLOCK

function write_block(io::IO, data::Vector{UInt8})
    c = Compressor(6)
    out = Vector{UInt8}(undef, length(data) + 256)
    n_payload = compress!(c, out, data)::Int
    block_size = length(BLOCK_HEADER) + n_payload + 2 + 8
    crc = crc32(data)
    write(io, BLOCK_HEADER)
    write(io, htol(UInt16(block_size - 1)))
    write(io, view(out, 1:n_payload))
    write(io, htol(crc))
    write(io, length(data) % Int32)
    return flush(io)
end

block_content = [
    b"Hello, world!",
    b"more data",
    b"",
    b"x",
    b"",
    b"then some more",
    b"more content here",
    b"this is another block",
]

open("data/1.gz", "w") do io
    for i in block_content
        write_block(io, collect(i))
    end
    write(io, EOF_BLOCK)
    flush(io)
end
