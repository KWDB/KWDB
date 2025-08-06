# basic images
FROM ubuntu:22.04

# Install the necessary software packages
RUN apt-get update && \
    apt-get install -y tzdata libcurl4-openssl-dev python3 protobuf-compiler openjdk-8-jre wget libgomp1 squashfs-tools && \
    ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

# Copy and set up the startup script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the working directory
WORKDIR /home/inspur

# Expose service port
EXPOSE 26888

# set ENTRYPOINT
ENTRYPOINT ["/entrypoint.sh"]
