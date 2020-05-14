FROM openjdk:8-alpine

ADD target/cdr-gen-1.0-SNAPSHOT-jar-with-dependencies.jar /cdr.jar

ENTRYPOINT [ "/usr/bin/java", "-jar", "/cdr.jar" ]