# BGZFLib
BGFLib.jl is a package for reading and writing [BGZF files](https://en.wikipedia.org/wiki/BGZF).
See the sections in the side bar.

## Example use
```julia
# Decompress a file
BGZFReader(open("file.bam")) do reader
    decompressed = read(reader)
end

# Compress it again
BGZFWriter(open("another.bam", "w")) do reader
    write(reader, decompressed)
end
```

## Comparison with other packages
#### BGZFStreams.jl
1. BGZFLib exposes the `AbstractBufReader` interface from [BufferIO.jl](https://github.com/BioJulia/BufferIO.jl). This interface gives more control to the user and is better documented.

2. BGZFLib is faster. Here are some timings on my laptop to decompress a 3,9 GiB BAM file for BGZFStreams and BGZFLib, respectively:
```
threads BGZFStreams BGZFLib
1             27.0    12.3
2             15.3     6.50
4              9.16    3.45
8              6.73    2.33
```

3. BGFLib is async, in the sense that (de)compression happens concurrent with reading and writing. In contrast, BGZFStream pauses reading/writing to (de)compress in parallel. 

#### CodecBGZF.jl
CodecBGZF.jl is deprecated and should not be used.