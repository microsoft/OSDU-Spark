FROM ubuntu:20.04

ENV LC_ALL C.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en

ARG SBT_VERSION=1.6.2

RUN apt-get update && \
    apt install --yes --no-install-recommends \
        # Generic requirements
        ca-certificates \
        # Java (default-jdk === OpenJDK)
        openjdk-11-jdk openjdk-11-source \
        git python3 python3-pip \
        # Required to run apt-get in the container
        sudo \
        # sbt
        apt-transport-https curl gnupg \
 # download SBT
 && curl -L -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb \
 && dpkg -i sbt-$SBT_VERSION.deb \
 && rm sbt-$SBT_VERSION.deb \
 # install SBT
 && apt-get update \
 && apt-get install --yes --no-install-recommends sbt \
 # Do this cleanup every time to ensure minimal layer sizes
 && apt-get clean autoclean \
 && apt-get autoremove -y \
 && rm -rf /var/lib/{apt,dpkg,cache,log}

# Non-layer configuration
# Set environment variables used by build
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64/"