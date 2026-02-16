# Shinka - GitOps-native Database Migration Operator
#
# Kubernetes operator that handles database migrations as part of GitOps workflows.
# Uses crate2nix for per-crate caching via Attic.
{
  self,
  inputs,
  ...
}: {
  perSystem = {
    config,
    system,
    pkgs,
    inputs',
    ...
  }: let
    # Import crate2nix
    crate2nix = inputs'.crate2nix.packages.default;

    # Import nix-lib for GHCR token and Rust overlay (host system)
    nixLibHost = import (self + "/pkgs/products/novaskyn/services/rust/nix-lib/lib/default.nix") {
      inherit pkgs system crate2nix;
    };
    defaultGhcrToken = nixLibHost.defaultGhcrToken;

    # On Mac, target Linux for Docker images (uses remote builder via /etc/nix/machines)
    # targetSystem =
    #   if pkgs.stdenv.isDarwin
    #   then "x86_64-linux"
    #   else system;
    targetPkgs =
      if pkgs.stdenv.isDarwin
      then
        import inputs.nixpkgs.outPath {
          system = "x86_64-linux";
          overlays = [
            (nixLibHost.mkRustOverlay {
              fenix = inputs.fenix;
              system = "x86_64-linux";
            })
          ];
        }
      else pkgs;

    # Shinka source location
    shinkaDir = self + "/pkgs/platform/shinka";
    shinkaCargoNix = shinkaDir + "/Cargo.nix";

    # Get nexus-deploy binary
    nexusDeployBinary = config.packages.nexus-deploy or null;

    # Build Shinka using crate2nix (on target system for Docker images)
    shinkaProject = import shinkaCargoNix {
      pkgs = targetPkgs;
      defaultCrateOverrides = targetPkgs.defaultCrateOverrides;
    };

    shinkaBinary = shinkaProject.rootCrate.build;

    # Build Shinka Docker image (on target system)
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
          "org.opencontainers.image.source" = "https://github.com/pleme-io/nexus";
          "org.opencontainers.image.vendor" = "Pleme";
        };
      };
    };
  in {
    # Export Shinka packages
    packages = {
      shinka = shinkaBinary;
      shinka-image = shinkaImage;
    };

    # Export Shinka apps
    apps = {
      # Push Shinka Docker image to GHCR
      "push:platform:shinka" = {
        type = "app";
        program = toString (pkgs.writeShellScript "push-shinka" ''
          set -euo pipefail
          export GITHUB_TOKEN="${defaultGhcrToken}"
          export GHCR_TOKEN="${defaultGhcrToken}"
          exec ${nexusDeployBinary}/bin/nexus-deploy push \
            --image-path "${shinkaImage}" \
            --registry "ghcr.io/pleme-io/shinka" \
            --auto-tags \
            --retries 3
        '');
      };

      # Regenerate Shinka Cargo.nix
      "regen:platform:shinka" = {
        type = "app";
        program = toString (pkgs.writeShellScript "regen-shinka" ''
          export SERVICE_DIR="pkgs/platform/shinka"
          export CARGO="${pkgs.cargo}/bin/cargo"
          export CRATE2NIX="${crate2nix}/bin/crate2nix"
          exec ${nexusDeployBinary}/bin/nexus-deploy bootstrap regenerate
        '');
      };
    };
  };
}
