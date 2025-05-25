FROM ubuntu:latest
LABEL authors="william"

ENTRYPOINT ["top", "-b"]