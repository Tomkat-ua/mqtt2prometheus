container=mqtt2prom
img=tomkat/mqtt2prom

docker build -t $img .


docker container stop $container
docker container rm $container

#ports - host:container
docker run -d -t  \
    --net net_18  --ip 172.18.0.21 \
    -p 8081:8081 \
    --restart always \
    --name $container \
    -e TZ=Europe/Kiev \
       $img