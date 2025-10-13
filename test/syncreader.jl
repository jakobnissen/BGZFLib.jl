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
@testset "SyncBGZFReader - virtual_position and virtual_seek" begin
    reader = SyncBGZFReader(CursorReader(gz1_data))

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

@testset "Reading" begin
    @testset "Read beyond EOF" begin
        reader = SyncBGZFReader(CursorReader(gz1_data))
        seekstart(reader)
        read(reader)
        @test read(reader, 100) == UInt8[]
        @test eof(reader)
        close(reader)
    end

    @testset "Read single byte" begin
        reader = SyncBGZFReader(CursorReader(gz1_data))
        @test read(reader, UInt8) == UInt8('H')
        @test read(reader, UInt8) == UInt8('e')
    end
end

@testset "SyncBGZFReader - get_buffer" begin
    reader = SyncBGZFReader(CursorReader(gz1_data))

    @testset "Initially empty" begin
        buffer = get_buffer(reader)
        @test length(buffer) == 0
        @test buffer isa ImmutableMemoryView{UInt8}
    end

    @testset "After fill_buffer" begin
        n = fill_buffer(reader)
        @test n > 0
        buffer = get_buffer(reader)
        @test length(buffer) == n
        @test buffer == gz1_content[1]
    end

    @testset "After partial consume" begin
        seekstart(reader)
        fill_buffer(reader)
        consume(reader, 5)
        buffer = get_buffer(reader)
        @test buffer == b", world!"
    end

    close(reader)
end

@testset "SyncBGZFReader - fill_buffer" begin
    reader = SyncBGZFReader(CursorReader(gz1_data))

    @testset "First fill" begin
        n = fill_buffer(reader)
        @test n > 0
        @test n == length(gz1_content[1])
        @test get_buffer(reader) == gz1_content[1]
    end

    @testset "Fill with non-empty buffer returns nothing" begin
        seekstart(reader)
        fill_buffer(reader)
        result = fill_buffer(reader)
        @test result === nothing
    end

    @testset "Fill after consume" begin
        seekstart(reader)
        fill_buffer(reader)
        first_block = get_buffer(reader)
        consume(reader, length(first_block))

        n = fill_buffer(reader)
        @test n == length(gz1_content[2])
        @test get_buffer(reader) == gz1_content[2]
    end

    @testset "Fill at EOF returns 0" begin
        seekstart(reader)
        for content in gz1_content
            fill_buffer(reader)
            consume(reader, length(get_buffer(reader)))
        end
        @test fill_buffer(reader) == 0
        @test eof(reader)
    end

    close(reader)
end

@testset "SyncBGZFReader - consume" begin
    reader = SyncBGZFReader(CursorReader(gz1_data))

    @testset "Consume partial buffer" begin
        fill_buffer(reader)
        initial_len = length(get_buffer(reader))
        consume(reader, 5)
        @test length(get_buffer(reader)) == initial_len - 5
    end

    @testset "Consume entire buffer" begin
        seekstart(reader)
        fill_buffer(reader)
        buf_len = length(get_buffer(reader))
        consume(reader, buf_len)
        @test length(get_buffer(reader)) == 0
    end

    @testset "Consume zero bytes" begin
        seekstart(reader)
        fill_buffer(reader)
        initial_len = length(get_buffer(reader))
        consume(reader, 0)
        @test length(get_buffer(reader)) == initial_len
    end

    @testset "Consume negative throws error" begin
        seekstart(reader)
        fill_buffer(reader)
        @test_throws IOError consume(reader, -1)
    end

    @testset "Consume more than available throws error" begin
        seekstart(reader)
        fill_buffer(reader)
        buf_len = length(get_buffer(reader))
        @test_throws IOError consume(reader, buf_len + 1)
    end

    @testset "Data correctness after consume" begin
        seekstart(reader)
        fill_buffer(reader)
        consume(reader, 7)
        buffer = get_buffer(reader)
        @test buffer == b"world!"
    end

    close(reader)
end

@testset "Error recovery with seek" begin
    data = append!(copy(gz1_data), b"bad data")
    reader = SyncBGZFReader(CursorReader(data))
    @test_throws BGZFError read(reader)
    @test_throws Exception fill_buffer(reader)

    seekstart(reader)
    @test read(reader, 10) == b"Hello, wor"
end

@testset "show" begin
    # We just test that showing doesn't error
    buf = IOBuffer()
    reader = SyncBGZFReader(CursorReader(UInt8[]))
    show(buf, reader)
    @test !isempty(take!(buf))
end
