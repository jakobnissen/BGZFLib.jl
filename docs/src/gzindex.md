```@meta
DocTestSetup = quote
    using MemoryViews
    using BufferIO
    using LibDeflate
    using BGZFLib

    (path_to_bgzf, bgzf_data, path_to_gzi, gzi_data) = let
        dir = joinpath(Base.pkgdir(BGZFLib), "data")
        path_to_bgzf = joinpath(dir, "1.gz")
        bgzf_data = open(read, path_to_bgzf)
        path_to_gzi = joinpath(dir, "1.gzi")
        gzi_data = open(read, path_to_gzi)
        (path_to_bgzf, bgzf_data, path_to_gzi, gzi_data)
    end
end
```

# Seeking BGZF files
Due to their blocked nature, it is possible to seek BGZF files, and to index them to support seeking to any position in the equivalent _decompressed_ stream.

Currently, only BGZF reader support seeking. Seek support for writers may be added in the future.

### Virtual seeking
A BGZF reader is seeked with a _virtual offset_, which contains two offsets: The _file offset_, which is the offset in the compressed stream that marks the start of the compressed BGZF block, and the _block offset_, the offset in the decompressed content of that block.

This is modeled by the `VirtualOffset`:

```@docs; canonical = false
VirtualOffset
```

The current `VirtualOffset` position is obtained with `virtual_position`, and seeking is done with `virtual_seek`:

```@docs; canonical = false
virtual_position
virtual_seek
```

BGZF readers also supports `Base.seek`. Calling `seek(io, x)` is equivalent to `virtual_seek(io, VirtualOffset(x, 0))`:

```@docs; canonical = false
Base.seek(::SyncBGZFReader, ::Int)
```

### Seeking with `GZIndex`
In order to seek to a certain *decompressed offset*, e.g. to seek to the 10,000th byte in a decompressed stream, you need to know the offset of the BGZF block that contains this byte in the *compressed stream*.
This can be efficiently obtained with a `GZIndex`.

An `index::GZIndex` value contains the (public) property `index.blocks`, which is a `Vector{@NamedTuple{compressed_offset::UInt64, decompressed_offset::UInt64}}`, with one element for each block in the corresponding file. All the values of `compressed_offset` and `decompressed_offset` are guaranteed to be sorted in ascending order in a `GZIndex`:

```@docs; canonical = false
GZIndex
```

Let's say you want to seek to the decompressed offset 37 in our example BGZF file.
The approach is this:

* Find the first block with a decompressed offset `D <= 37`. Since `.blocks` is sorted, you can use binary search for this.
* Virtualseek to `VirtalOffset(D, 37 - D)`

```jldoctest
gzi = load_gzi(CursorReader(gzi_data))
reader = SyncBGZFReader(CursorReader(bgzf_data))

# Use `searchsortedlast` to find the last block with a decompressed offset
# <= 37, i.e. the block containing the decompressed offset 37.
target_block = (;compressed_offset=0, decompressed_offset=37)
idx = searchsortedlast(gzi.blocks, target_block, by=i -> i.decompressed_offset)
(;compressed_offset, decompressed_offset) = gzi.blocks[idx]
virtual_seek(reader, VirtualOffset(compressed_offset, 37 - decompressed_offset))
read(reader) |> String

# output
"more content herethis is another block"
```

#### Building a `GZIndex`
A GZIndex can be constructed manually from a correct (and sorted) vector `v` of the above mentioned type using `GZIndex(v)`. More commonly, it is either computed from a BGZF file, or directly loaded from a GZI file:

```@docs; canonical = false
index_bgzf
load_gzi
```

#### Writing GZI files
This is done with `write_gzi`:

```@docs; canonical = false
write_gzi
```
