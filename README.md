# PBFT Simple Implementation

## Execution
```
make up
make test
```

## Monitoring
You can monitor the execution of nodes using grafana and prometheus. To do so, run the following commands:
```
docker-compose up -d
```
This brings up a grafana instance on `localhost:3000` and a prometheus instance on `localhost:9090`. You can access the grafana instance using the credentials `admin:admin`. The prometheus instance is used to scrape metrics from the nodes.
