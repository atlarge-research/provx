up: build (upload "das") (upload "das6")

build:
  sbt assembly

upload target:
  rsync -azP target/scala-2.13/provxlib-assembly-0.1.0-SNAPSHOT.jar {{target}}:provxlib/run

