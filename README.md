# TinyUp

A TinySQL deployment tool inspired by [TiUP](https://github.com/pingcap/tiup/).

## Build

```bash
go build
./tinyup -h
simple cluster operation tool

Usage:
  cluster [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  deploy      deploy a local cluster
  destroy     destroy the cluster
  help        Help about any command
  start       start the cluster
  stop        stop the cluster
  upgrade     upgrade the cluster

Flags:
  -b, --binary_path string   the binary path (default "./bin")
  -d, --deploy_path string   the deploy path (default "./bin/deploy")
  -h, --help                 help for cluster
  -n, --num int              the number of the tinykv servers (default 3)

Use "cluster [command] --help" for more information about a command.
```

## Deploy

Before starting the cluster, deployment is required.

```bash
mkdir -p bin
cp {path-to-tinysql-binary} ./bin
cp {path-to-tinykv-binary} ./bin
cp {path-to-tinyscheduler-binary} ./bin
./tinyup deploy
```

## Start

```bash
./tinyup start
# After the cluster is ready, use mysql-client to connect to it
mysql -h 127.0.0.1 -P4000 -uroot -Dtest
```

## Stop

```bash
./tinyup stop
```

## Destroy

```bash
./tinyup destroy
```
