@testset "VirtualOffset" begin
    @testset "Construction" begin
        vo = VirtualOffset(100, 50)
        @test vo.file_offset == 100
        @test vo.block_offset == 50
    end

    @testset "Valid ranges" begin
        # Maximum valid file offset: 2^48 - 1
        vo1 = VirtualOffset(2^48 - 1, 0)
        @test vo1.file_offset == 2^48 - 1

        # Maximum valid block offset: 2^16 - 1
        vo2 = VirtualOffset(0, 2^16 - 1)
        @test vo2.block_offset == 2^16 - 1
    end

    @testset "Out of range throws error" begin
        # File offset too large
        @test_throws ArgumentError VirtualOffset(2^48, 0)

        # Block offset too large
        @test_throws ArgumentError VirtualOffset(0, 2^16)
    end

    @testset "Comparison" begin
        vo1 = VirtualOffset(100, 10)
        vo2 = VirtualOffset(100, 20)
        vo3 = VirtualOffset(200, 5)
        vo4 = VirtualOffset(100, 10)

        @test vo1 < vo2
        @test vo1 < vo3
        @test vo2 < vo3
        @test !(vo1 < vo4)

        @test cmp(vo1, vo2) == -1
        @test cmp(vo2, vo1) == 1
        @test cmp(vo1, vo4) == 0
    end

    @testset "Show" begin
        vo = VirtualOffset(2^48 - 1, 2^16 - 1)
        io = IOBuffer()
        show(io, vo)
        @test !isempty(take!(io))
    end
end


@testset "GZIndex constructor" begin
    @testset "Valid sorted blocks" begin
        blocks = [
            (compressed_offset = 0x0000000000000000, decompressed_offset = 0x0000000000000000),
            (compressed_offset = 0x000000000000002c, decompressed_offset = 0x000000000000000d),
            (compressed_offset = 0x0000000000000058, decompressed_offset = 0x0000000000000016),
        ]

        gzi = GZIndex(blocks)
        @test gzi isa GZIndex
        @test gzi.blocks == blocks
    end

    @testset "Unsorted compressed offsets throws error" begin
        blocks = [
            (compressed_offset = 0x0000000000000058, decompressed_offset = 0x0000000000000000),
            (compressed_offset = 0x000000000000002c, decompressed_offset = 0x000000000000000d),
        ]

        @test_throws BGZFError GZIndex(blocks)
    end

    @testset "Unsorted decompressed offsets throws error" begin
        blocks = [
            (compressed_offset = 0x000000000000002c, decompressed_offset = 0x000000000000000d),
            (compressed_offset = 0x0000000000000058, decompressed_offset = 0x0000000000000000),
        ]

        @test_throws BGZFError GZIndex(blocks)
    end

    @testset "Empty blocks vector" begin
        blocks = typeof((compressed_offset = UInt64(0), decompressed_offset = UInt64(0)))[]
        gzi = GZIndex(blocks)
        @test gzi.blocks == blocks
    end
end

@testset "load_gzi" begin
    @testset "From AbstractBufReader" begin
        gzi = load_gzi(CursorReader(gzi_data))
        @test gzi isa GZIndex
        @test !isempty(gzi.blocks)
        @test all(i -> gzi.blocks[i].compressed_offset <= gzi.blocks[i + 1].compressed_offset, 1:(length(gzi.blocks) - 1))
        @test all(i -> gzi.blocks[i].decompressed_offset <= gzi.blocks[i + 1].decompressed_offset, 1:(length(gzi.blocks) - 1))
    end

    @testset "From IO" begin
        gzi = load_gzi(IOBuffer(gzi_data))
        @test gzi isa GZIndex
        @test !isempty(gzi.blocks)
    end

    @testset "Truncated file throws EOF error" begin
        # Remove some bytes from the end
        truncated = gzi_data[1:(end - 10)]
        @test_throws IOError load_gzi(CursorReader(truncated))
    end

    @testset "Empty file throws EOF error" begin
        @test_throws IOError load_gzi(CursorReader(UInt8[]))
    end
end

@testset "index_bgzf" begin
    @testset "From AbstractBufReader" begin
        gzi = index_bgzf(CursorReader(gz1_data))
        @test gzi isa GZIndex
        @test !isempty(gzi.blocks)

        # First block should start at offset 0
        @test gzi.blocks[1].compressed_offset == 0
        @test gzi.blocks[1].decompressed_offset == 0

        # Blocks should be sorted
        @test all(i -> gzi.blocks[i].compressed_offset <= gzi.blocks[i + 1].compressed_offset, 1:(length(gzi.blocks) - 1))
        @test all(i -> gzi.blocks[i].decompressed_offset <= gzi.blocks[i + 1].decompressed_offset, 1:(length(gzi.blocks) - 1))
    end

    @testset "From IO" begin
        gzi = index_bgzf(IOBuffer(gz1_data))
        @test gzi isa GZIndex
        @test !isempty(gzi.blocks)
    end

    @testset "Index matches loaded gzi" begin
        computed_gzi = index_bgzf(CursorReader(gz1_data))
        loaded_gzi = load_gzi(CursorReader(gzi_data))

        @test computed_gzi.blocks == loaded_gzi.blocks
    end

    @testset "Empty file returns 1-element index" begin
        empty_bgzf = BGZFLib.EOF_BLOCK
        gzi = index_bgzf(CursorReader(empty_bgzf))
        @test gzi.blocks == [(compressed_offset = 0x0000000000000000, decompressed_offset = 0x0000000000000000)]
    end

    @testset "Use index for seeking" begin
        gzi = index_bgzf(CursorReader(gz1_data))
        reader = SyncBGZFReader(CursorReader(gz1_data))
        decompressed = read(reader)

        # Read from all the blocks in the index
        for (; compressed_offset, decompressed_offset) in gzi.blocks
            virtual_seek(reader, VirtualOffset(compressed_offset, 0))
            @test read(reader) == decompressed[(decompressed_offset + 1):end]
        end

        close(reader)
    end
end

@testset "Round trip: index, write, load" begin
    # Create an index from the BGZF file
    original_gzi = index_bgzf(CursorReader(gz1_data))

    # Write it to a buffer
    io = VecWriter()
    write_gzi(io, original_gzi)

    # Load it back
    loaded_gzi = load_gzi(CursorReader(io.vec))

    # Should match
    @test original_gzi.blocks == loaded_gzi.blocks
end
