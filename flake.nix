{
  description = "Shinka - GitOps-native database migration operator for Kubernetes";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/d6c71932130818840fc8fe9509cf50be8c64634f";

    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    crate2nix,
    ...
  }: let
    systems = ["aarch64-darwin" "x86_64-linux" "aarch64-linux"];
    linuxSystems = ["x86_64-linux" "aarch64-linux"];
    forAllSystems = f:
      nixpkgs.lib.genAttrs systems (system:
        f {
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };
          inherit system;
        });
    forLinuxSystems = f:
      nixpkgs.lib.genAttrs linuxSystems (system:
        f {
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };
          inherit system;
        });
  in {
    # Binary and Docker image packages are Linux-only (Docker images target Linux)
    packages = forLinuxSystems ({
      pkgs,
      system,
    }: let
      shinkaProject = import (self + "/Cargo.nix") {
        inherit pkgs;
        defaultCrateOverrides = pkgs.defaultCrateOverrides;
      };

      shinkaBinary = shinkaProject.rootCrate.build;

      shinkaImage = pkgs.dockerTools.buildLayeredImage {
        name = "ghcr.io/pleme-io/shinka";
        tag = "latest";
        contents = [shinkaBinary pkgs.cacert];
        config = {
          Entrypoint = ["${shinkaBinary}/bin/shinka"];
          Env = [
            "RUST_LOG=info,shinka=debug"
            "LOG_FORMAT=json"
            "HEALTH_ADDR=0.0.0.0:8080"
            "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
          ];
          ExposedPorts = {
            "8080/tcp" = {};
          };
          Labels = {
            "org.opencontainers.image.title" = "shinka";
            "org.opencontainers.image.description" = "GitOps-native database migration operator for Kubernetes";
            "org.opencontainers.image.source" = "https://github.com/pleme-io/shinka";
            "org.opencontainers.image.vendor" = "Pleme";
          };
        };
      };
    in {
      shinka = shinkaBinary;
      shinka-image = shinkaImage;
    });

    apps = forAllSystems ({
      pkgs,
      system,
    }: let
      crate2nixPkg = crate2nix.packages.${system}.default;
    in {
      "regen" = {
        type = "app";
        program = toString (pkgs.writeShellScript "regen-shinka" ''
          export SERVICE_DIR="."
          export CARGO="${pkgs.cargo}/bin/cargo"
          export CRATE2NIX="${crate2nixPkg}/bin/crate2nix"
          cd "$(git rev-parse --show-toplevel)"
          ${crate2nixPkg}/bin/crate2nix generate
        '');
      };
    });
  };
}
