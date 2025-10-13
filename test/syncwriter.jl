@testset "From AbstractBufWriter" begin
    io = VecWriter()
    writer = SyncBGZFWriter(io)
    @test writer isa SyncBGZFWriter{VecWriter}
    @test write(writer, b"test data") == 9
    close(writer)

    reader = BGZFReader(CursorReader(io.vec))
    @test read(reader) == b"test data"
    close(reader)
end

@testset "From IO" begin
    io = IOBuffer()
    writer = SyncBGZFWriter(io)
    @test writer isa SyncBGZFWriter{BufWriter{IOBuffer}}
    @test write(writer, b"test data") == 9
    flush(writer)
    seekstart(io)
    data = read(io)
    append!(data, BGZFLib.EOF_BLOCK)
    close(writer)
    reader = SyncBGZFReader(CursorReader(data))
    @test read(reader) == b"test data"
    close(reader)
end

@testset "Write and read back" begin
    @testset "Multiple writes" begin
        io = VecWriter()
        writer = SyncBGZFWriter(io)
        write(writer, b"Hello, ")
        write(writer, b"world!")
        write(writer, b" More data.")
        close(writer)

        reader = SyncBGZFReader(CursorReader(io.vec))
        @test read(reader) == b"Hello, world! More data."
        close(reader)
    end

    @testset "Empty write" begin
        io = VecWriter()
        writer = SyncBGZFWriter(io)
        close(writer)

        @test io.vec == BGZFLib.EOF_BLOCK
        reader = SyncBGZFReader(CursorReader(io.vec))
        @test read(reader) == UInt8[]
        close(reader)
    end
end

@testset "Large write" begin
    io = VecWriter()
    writer = SyncBGZFWriter(io)

    # Create data larger than 16 KiB (65536 bytes)
    data = repeat(b"0123456789", 7000)  # 70000 bytes
    @test length(data) > 2^16

    @test write(writer, data) == length(data)
    close(writer)

    reader = SyncBGZFReader(CursorReader(io.vec))
    @test read(reader) == data
    close(reader)
end

@testset "Without empty block fails check_truncated" begin
    io = VecWriter()
    writer = SyncBGZFWriter(io; append_empty = false)
    write(writer, b"test data")
    close(writer)

    # Should fail with check_truncated=true
    reader = SyncBGZFReader(CursorReader(io.vec); check_truncated = true)
    @test_throws BGZFError read(reader)
    close(reader)

    # Should succeed with check_truncated=false
    reader = SyncBGZFReader(CursorReader(io.vec); check_truncated = false)
    @test read(reader) == b"test data"
    close(reader)
end

@testset "Manual write_empty_block passes check_truncated" begin
    io = VecWriter()
    writer = SyncBGZFWriter(io; append_empty = false)
    write(writer, b"test data")
    write_empty_block(writer)
    close(writer)

    # Should succeed with check_truncated=true now
    reader = SyncBGZFReader(CursorReader(io.vec); check_truncated = true)
    @test read(reader) == b"test data"
    close(reader)
end

@testset "Function argument constructor" begin
    io = VecWriter()

    result = SyncBGZFWriter(io; append_empty = true, compresslevel = 6) do writer
        @test isopen(writer)
        @test write(writer, b"Hello, ") == 7
        @test write(writer, b"world!") == 6
    end

    # Verify the writer was properly closed
    reader = SyncBGZFReader(CursorReader(io.vec); check_truncated = true)
    @test read(reader) == b"Hello, world!"
    close(reader)
end

@testset "show" begin
    # We just test that showing doesn't error
    buf = IOBuffer()
    writer = SyncBGZFWriter(VecWriter())
    show(buf, writer)
    @test !isempty(take!(buf))
end
