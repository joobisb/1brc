# The One Billion Row Challenge

Go Implementation of [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc)

## Run benchmarks/profiling

`go test -bench=.`

`go tool pprof -web cpu.pprof`

`go tool pprof -http=:8080 cpu.pprof`