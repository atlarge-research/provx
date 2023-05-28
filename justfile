build:
  sbt compile

push:
  #!/usr/bin/env bash
  function dcp() {
    bash /Users/gm/.ssh/vu/pass.sh > /dev/null && SSH_ASKPASS=/Users/gm/.ssh/vu/pass.sh SSH_ASKPASS_REQUIRE=force scp $@
  }
  sbt assembly && dcp target/scala-2.12/provxlib-assembly-0.1.0-SNAPSHOT.jar das:provx-run
