# BGZFLib.jl
[![Stable docs](https://img.shields.io/badge/docs-stable-blue.svg)](https://biojulia.dev/BGZFLib.jl/stable/)
[![Dev docs](https://img.shields.io/badge/docs-latest-blue.svg)](https://biojulia.dev/BGZFLib.jl/dev/)
[![Latest Release](https://img.shields.io/github/release/BioJulia/BGZFLib.jl.svg)](https://github.com/BioJulia/BGZFLib.jl/releases/latest)
[![](https://codecov.io/gh/BioJulia/BGZFLib.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/BioJulia/BGZFLib.jl)

BGZFLib.jl is a package for reading and writing [Blocked GNU Zip Format (BGZF)](https://en.wikipedia.org/wiki/BGZF) files.

# Example
### Reading a BGZF file:
```shell
$ echo "Hello, world" | bgzip > /tmp/foo.gz
```

```julia
julia> using BGZFLib

julia> BGZFReader(read, open("/tmp/foo.gz")) |> String
"Hello, world\n"
```

### Writing a BGZF file
```julia
julia> using BGZFLib

julia> BGZFWriter(io -> write(io, "Hello, world"), open("/tmp/foo.gz", "w"))
12
```

```shell
$ bgzip -dc /tmp/foo.gz
Hello, world
```

## Questions?
If you have a question about contributing or using BioJulia software, come on over and chat to us on [the Julia Slack workspace](https://julialang.org/slack/), or you can try the [Bio category of the Julia discourse site](https://discourse.julialang.org/c/domain/bio).
