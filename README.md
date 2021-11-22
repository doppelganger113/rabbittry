# rabbittry
Example RabbitMQ consumer and producer with retry, implemented in Go

## Starting

To start locally execute:
```bash
docker-compose up -d
```

To test network issues with Rabbit execute:
```bash
# List networks and grab network id of network used for rabbitmq container
docker network ls
# List running containers to grab Rabbit container id
docker ps
# disconnect or connect network with container
docker network disconnect [network id] [container id]
```

## Killing a process
```bash
# Find go process 
ps gx | grep go
kill [process id]
```
