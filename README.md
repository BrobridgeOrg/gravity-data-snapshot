# Gravity Data Snapshot

Data snapshot component is used to realize efficient data recovery for CQRS. Event though event sourcing can be used to recovery data, it spends much time on re-playing events. In this case, it lead to slow data sycing for creating a new data database or rebuild a new one.

## Update proto definition

Rebuild to apply `proto` changes, just run commands below:

```shell
cd pb
protoc --go_out=plugins=grpc:. *.proto
```

## License

Licensed under the MIT License

## Authors

Copyright(c) 2020 Fred Chien <<fred@brobridge.com>>
