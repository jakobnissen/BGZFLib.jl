using BGZFLib
using Test
using BufIO: consume, fill_buffer, get_buffer

@testset "Basic reading" begin
    block_content_1 = [
        b"Hello, world!",
        b"more data",
        b"",
        b"x",
        b"",
        b"then some more",
        b"more content here",
        b"this is another block",
    ] |> filter(!isempty)

    DIR = joinpath(dirname(dirname(pathof(BGZFLib))), "data")

    indata = open(read, joinpath(DIR, "1.gz"))

    for n_threads in [1, 2, 4, 16]
        io = IOBuffer(indata)
        reader = BGZFReader(io; threads = n_threads)

        for data in block_content_1
            @test isempty(get_buffer(reader))
            @test !iszero(fill_buffer(reader))
            @test get_buffer(reader) == data
            consume(reader, length(data))
        end

        @test eof(reader)
        @test iszero(fill_buffer(reader))
    end
end

# TODO:
# * No EOF block check
# * Empty input
