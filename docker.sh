docker stop shiva
docker rm shiva
docker rmi shiva
docker build -t shiva .
docker run -d --name shiva -p 8080:8080 -v /Users/alexey/Documents/clickberry/shiva/test:/data -e NSQD_ADDRESS=bus.qa.clbr.ws -e NSQLOOKUPD_ADDRESSES=bus1.qa.clbr.ws:4161,bus2.qa.clbr.ws:4161 -e S3_BUCKET=clickberryshiva shiva
