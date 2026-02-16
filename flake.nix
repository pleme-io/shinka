{
  description = "Shinka - GitOps-native database migration operator for Kubernetes";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/d6c71932130818840fc8fe9509cf50be8c64634f";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    fenix,
    crate2nix,
    ...
  }: let
    systems = ["aarch64-darwin" "x86_64-linux" "aarch64-linux"];
    forAllSystems = f:
      nixpkgs.lib.genAttrs systems (system:
        f {
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };
          inherit system;
        });
  in {
    packages = forAllSystems ({
      pkgs,
      system,
    }: let
      crate2nixPkg = crate2nix.packages.${system}.default;

      # Rust overlay for the target system
      mkRustOverlay = targetSystem: final: prev: let
        fenixPkgs = fenix.packages.${targetSystem};
        toolchain = fenixPkgs.stable.defaultToolchain;
      in {
        rustc = toolchain;
        cargo = toolchain;
      };

      # Target Linux for Docker images when building on Mac
      targetPkgs =
        if pkgs.stdenv.isDarwin
        then
          import nixpkgs.outPath {
            system = "x86_64-linux";
            overlays = [(mkRustOverlay "x86_64-linux")];
          }
        else pkgs;

      cargoNix = self + "/Cargo.nix";

      shinkaProject = import cargoNix {
        pkgs = targetPkgs;
        defaultCrateOverrides = targetPkgs.defaultCrateOverrides;
      };

      shinkaBinary = shinkaProject.rootCrate.build;

      shinkaImage = targetPkgs.dockerTools.buildLayeredImage {
        name = "ghcr.io/pleme-io/shinka";
        tag = "latest";
        contents = [shinkaBinary targetPkgs.cacert];
        config = {
          Entrypoint = ["${shinkaBinary}/bin/shinka"];
          Env = [
            "RUST_LOG=info,shinka=debug"
            "LOG_FORMAT=json"
            "HEALTH_ADDR=0.0.0.0:8080"
            "SSL_CERT_FILE=${targetPkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
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
