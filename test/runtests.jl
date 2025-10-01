using BGZFLib
using Test
using BufferIO: consume, fill_buffer, get_buffer, CursorReader, BufReader

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
    @testset "From AbstractBufReader" begin
        reader = SyncBGZFReader(CursorReader(gz1_data))
        @test reader isa SyncBGZFReader{CursorReader}
        @test read(reader) == reduce(vcat, gz1_content)
    end

    @testset "From IO" begin
        reader = SyncBGZFReader(IOBuffer(gz1_data))
        @test reader isa SyncBGZFReader{BufReader{IOBuffer}}
        @test read(reader) == reduce(vcat, gz1_content)
    end

    # From this point on, we assume the reader behaves the same whether
    # it's backed by an IO or an AbstractBufReader type, and we will only
    # test with `CursorReader`.
    @testset "Seeking and VirtualOffset" begin
        @testset "virtual_position" begin
            reader = SyncBGZFReader(CursorReader(gz1_data))

            # Start position is (0, 0)
            vo = virtual_position(reader)
            @test (vo.file_offset, vo.block_offset) == (0, 0)

            @test read(reader, 10) == b"Hello, wor"
            vo = virtual_position(reader)
            @test (vo.file_offset, vo.block_offset) == (0, 10) # still in block 1

            @test read(reader, 3) == b"ld!"
            vo = virtual_position(reader)
            @test (vo.file_offset, vo.block_offset) == (0, 13) # still in block 1

            @test read(reader, UInt8) == UInt8('m') # first byte of block 2
            vo = virtual_position(reader)
            @test vo.file_offset > 28 # minimum block size is 28
            @test vo.block_offset == 1
        end

        @testset "Seeking" begin
            # Seek to start
            reader = SyncBGZFReader(CursorReader(gz1_data))
            @test read(reader, 17) == b"Hello, world!more"
            seekstart(reader)
            @test read(reader, 5) == b"Hello"

            # Seek to a known position: Second block begins at fille offset 44.
            seek(reader, 44)
            @test read(reader, 10) == b"more datax"

            # Seek to a virtual position
            virtual_seek(reader, VirtualOffset(44, 5))
            @test read(reader, 6) == b"dataxt"
        end
    end
end

@testset "Basic reading" begin
    for n_workers in [1, 2, 4, 16]
        io = IOBuffer(gz1_data)
        reader = BGZFReader(io; n_workers)

        for data in gz1_content
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
