# 6.824 code

## Run

### Lab 1

```sh
cd src/main
go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run -race mrcoordinator.go pg-*.txt
# start workers in other windows
go run -race mrworker.go wc.so
cat mr-out-* | sort | more
```

## Test

### Lab 1

```sh
cd src/main
sh test-mr.sh
sh test-mr-many.sh <NumTest>
```

## Todo

- [x] Lab1
- [ ] Lab2
- [ ] Lab3
- [ ] Lab4
