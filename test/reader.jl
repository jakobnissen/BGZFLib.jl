@testset "From AbstractBufReader" begin
    for n_workers in [1, 4]
        reader = BGZFReader(CursorReader(gz1_data); n_workers)
        @test reader isa BGZFReader{CursorReader}
        @test read(reader) == reduce(vcat, gz1_content)
        close(reader)
    end
end

@testset "From IO" begin
    for n_workers in [1, 4]
        reader = BGZFReader(IOBuffer(gz1_data); n_workers)
        @test reader isa BGZFReader{BufReader{IOBuffer}}
        @test read(reader) == reduce(vcat, gz1_content)
        close(reader)
    end
end

@testset "Read entire file" begin
    for n_workers in [1, 4]
        reader = BGZFReader(CursorReader(gz1_data); n_workers)
        all_data = read(reader)
        @test all_data == reduce(vcat, gz1_content)
        close(reader)
    end
end

@testset "Read specific amounts" begin
    for n_workers in [1, 4]
        reader = BGZFReader(CursorReader(gz1_data); n_workers)

        chunk1 = read(reader, 5)
        @test chunk1 == b"Hello"

        chunk2 = read(reader, 8)
        @test chunk2 == b", world!"

        chunk3 = read(reader, 10)
        @test chunk3 == b"more datax"

        close(reader)
    end
end

@testset "Read single byte" begin
    for n_workers in [1, 4]
        reader = BGZFReader(CursorReader(gz1_data); n_workers)

        @test read(reader, UInt8) == UInt8('H')
        @test read(reader, UInt8) == UInt8('e')
        @test read(reader, UInt8) == UInt8('l')

        close(reader)
    end
end

@testset "BGZFReader - eof" begin
    for n_workers in [1, 4]
        reader = BGZFReader(CursorReader(gz1_data); n_workers)

        @test !eof(reader)

        read(reader, 10)
        @test !eof(reader)

        read(reader)
        @test eof(reader)

        @test read(reader, 100) == UInt8[]
        @test read(reader) == UInt8[]

        close(reader)
        @test !isopen(reader)
    end
end

@testset "Seeking" begin
    @testset "seekstart" begin
        for n_workers in [1, 4]
            reader = BGZFReader(CursorReader(gz1_data); n_workers)
            read(reader, 20)
            seekstart(reader)
            @test read(reader, 5) == b"Hello"

            seek(reader, 0)
            @test read(reader, 13) == b"Hello, world!"
            close(reader)
        end
    end
end

@testset "BGZFReader - virtual_position and virtual_seek" begin
    for n_workers in [1, 4]
        reader = BGZFReader(CursorReader(gz1_data); n_workers)

        @testset "Initial virtual position" begin
            vo = virtual_position(reader)
            @test vo.file_offset == 0
            @test vo.block_offset == 0
        end

        @testset "Virtual position after reading" begin
            read(reader, 5)
            vo = virtual_position(reader)
            @test vo.file_offset == 0
            @test vo.block_offset == 5

            read(reader, 8)
            vo = virtual_position(reader)
            @test vo.file_offset == 0
            @test vo.block_offset == 13
        end

        @testset "Virtual position across blocks" begin
            seekstart(reader)
            read(reader, 13)
            vo1 = virtual_position(reader)

            read(reader, 1)
            vo2 = virtual_position(reader)
            @test vo2.file_offset > vo1.file_offset
            @test vo2.block_offset == 1
        end

        @testset "Virtual seek to saved position" begin
            seekstart(reader)
            read(reader, 7)
            saved_vo = virtual_position(reader)

            read(reader, 10)
            virtual_seek(reader, saved_vo)

            @test virtual_position(reader) == saved_vo
            @test read(reader, 6) == b"world!"
        end

        @testset "Virtual seek to beginning" begin
            read(reader, 20)
            virtual_seek(reader, VirtualOffset(0, 0))

            @test virtual_position(reader) == VirtualOffset(0, 0)
            @test read(reader, 5) == b"Hello"
        end

        @testset "Virtual seek within same block" begin
            seekstart(reader)
            virtual_seek(reader, VirtualOffset(0, 7))

            @test read(reader, 6) == b"world!"
        end

        @testset "Virtual seek to different block" begin
            seekstart(reader)
            read(reader, 13)
            read(reader, 5)
            vo_second_block = virtual_position(reader)

            seekstart(reader)
            virtual_seek(reader, vo_second_block)

            @test virtual_position(reader) == vo_second_block
        end

        @testset "Virtual seek out of bounds throws error" begin
            seekstart(reader)
            @test_throws BGZFError virtual_seek(reader, VirtualOffset(0, 100))
        end

        close(reader)
    end
end

@testset "Error recovery with seek" begin
    for n_workers in [1, 4]
        data = append!(copy(gz1_data), b"bad data")
        reader = BGZFReader(CursorReader(data); n_workers)
        @test_throws BGZFError read(reader)
        @test_throws Exception fill_buffer(reader)

        seekstart(reader)
        @test read(reader, 10) == b"Hello, wor"
        n_bytes_in_data = sum(length, gz1_content)

        # No error, even as malformed block has been encountered.
        read(reader, n_bytes_in_data - 10)

        # Attempting to read one byte from malformed block errors.
        @test_throws BGZFError read(reader, UInt8)

        close(reader)
    end
end

@testset "Function argument constructor" begin
    io = IOBuffer(gz1_data)
    result = BGZFReader(io; n_workers = 2) do reader
        @test isopen(reader)
        @test read(reader, 5) == b"Hello"
        @test read(reader, 8) == b", world!"
        read(reader)
    end

    @test !isopen(io)
    @test result == b"more dataxthen some moremore content herethis is another block"
end

@testset "show" begin
    # We just test that showing doesn't error

end
