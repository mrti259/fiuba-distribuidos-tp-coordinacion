{ pkgs, lib, config, inputs, ... }:

{
  languages.python = {
    enable = true;
    venv.enable = true;
    venv.requirements = ./python/requirements.txt;
  };
  packages = with pkgs; [ python314Packages.pika ];

  git-hooks.hooks.ruff.enable = false;
}
