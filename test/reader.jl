@testset "Read entire file" begin
    for n_workers in [1, 4]
        reader = BGZFReader(IOBuffer(gz1_data); n_workers)
        all_data = read(reader)
        @test all_data == reduce(vcat, gz1_content)
        close(reader)
    end
end

@testset "Read specific amounts" begin
    for n_workers in [1, 4]
        reader = BGZFReader(IOBuffer(gz1_data); n_workers)

        chunk1 = read(reader, 5)
        @test chunk1 == b"Hello"

        chunk2 = read(reader, 8)
        @test chunk2 == b", world!"

        chunk3 = read(reader, 10)
        @test chunk3 == b"more datax"

        close(reader)
    end
end

@testset "Read single byte with" begin
    for n_workers in [1, 4]
        reader = BGZFReader(IOBuffer(gz1_data); n_workers)

        @test read(reader, UInt8) == UInt8('H')
        @test read(reader, UInt8) == UInt8('e')
        @test read(reader, UInt8) == UInt8('l')

        close(reader)
    end
end

@testset "BGZFReader - eof" begin
    @testset "EOF detection" begin
        reader = BGZFReader(IOBuffer(gz1_data); n_workers = 2)

        @test !eof(reader)

        read(reader, 10)
        @test !eof(reader)

        read(reader)
        @test eof(reader)

        @test read(reader, 100) == UInt8[]
        @test read(reader) == UInt8[]

        close(reader)
    end
end

@testset "BGZFReader - close" begin
    @testset "Close reader" begin
        reader = BGZFReader(IOBuffer(gz1_data); n_workers = 4)

        @test isopen(reader)
        close(reader)
        @test !isopen(reader)

        @test fill_buffer(reader) == 0
    end
end

@testset "seeking" begin
    @testset "seekstart" begin
        reader = BGZFReader(IOBuffer(gz1_data); n_workers = 4)
        read(reader, 20)
        seekstart(reader)
        @test read(reader, 5) == b"Hello"

        seek(reader, 0)
        @test read(reader, 13) == b"Hello, world!"
        close(reader)
    end
end
