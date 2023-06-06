{ pkgs, ... }:

{
  languages.python = {
    enable = true;

    version = "3.11";

    poetry = {
      enable = true;
      activate.enable = true;
      install.enable = true;
    };
  };
}
