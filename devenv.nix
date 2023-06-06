{ pkgs, ... }:

{
  packages = [
    pkgs.just
    pkgs.jq
  ];

  languages.java.jdk.package = pkgs.jdk8;
  languages.scala = {
    enable = true;
    package = pkgs.scala_2_12;
  };
}
