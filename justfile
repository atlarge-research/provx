build:
  sbt assembly

upload: build
  dsync target/scala-2.13/provxlib-assembly-0.1.0-SNAPSHOT.jar das:provxlib/run
