using BGZFLib
using Test
using MemoryViews
using BufferIO: consume, fill_buffer, get_buffer, CursorReader, BufReader, IOError

DIR = joinpath(dirname(dirname(pathof(BGZFLib))), "data")

# Test BGZF formatted files
gz1_data = open(read, joinpath(DIR, "1.gz"))

# Decompressed content of gz1, each block, in order
gz1_content = [
    b"Hello, world!",
    b"more data",
    b"",
    b"x",
    b"",
    b"then some more",
    b"more content here",
    b"this is another block",
] |> filter(!isempty)

@testset "SyncReader" begin
    include("syncreader.jl")
end

@testset "BGZFReader" begin
    include("reader.jl")
end

# TODO:
# * No EOF block check
# * Empty input
