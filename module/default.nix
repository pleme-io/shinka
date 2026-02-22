# Shinka home-manager module — daemon service
#
# Namespace: services.shinka.daemon.*
#
# Runs shinka in operator mode as a persistent service.
# Mode is controlled by RUN_MODE env var (default: operator).
#
# Module factory: receives { hmHelpers } from flake.nix, returns HM module.
{ hmHelpers }:
{
  lib,
  config,
  pkgs,
  ...
}:
with lib; let
  inherit (hmHelpers) mkLaunchdService mkSystemdService;
  cfg = config.services.shinka.daemon;
  isDarwin = pkgs.stdenv.isDarwin;
in {
  options.services.shinka.daemon = {
    enable = mkOption {
      type = types.bool;
      default = false;
      description = "Enable Shinka database migration operator";
    };

    package = mkOption {
      type = types.package;
      default = pkgs.shinka;
      description = "Shinka package";
    };

    healthAddr = mkOption {
      type = types.str;
      default = "0.0.0.0:8080";
      description = "Health + API server bind address";
    };

    metricsPort = mkOption {
      type = types.int;
      default = 9090;
      description = "Prometheus metrics port";
    };

    watchNamespace = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "Kubernetes namespace to watch (null = all namespaces)";
    };

    configFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "Path to shinka YAML config file";
    };

    logLevel = mkOption {
      type = types.str;
      default = "info";
      description = "Log level (info, debug, trace, etc.)";
    };

    logFormat = mkOption {
      type = types.enum ["json" "pretty"];
      default = "json";
      description = "Log output format";
    };

    discordWebhookUrl = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "Discord webhook URL for notifications";
    };

    extraEnv = mkOption {
      type = types.attrsOf types.str;
      default = {};
      description = "Additional environment variables";
    };
  };

  config = let
    env = {
      RUN_MODE = "operator";
      HEALTH_ADDR = cfg.healthAddr;
      METRICS_PORT = toString cfg.metricsPort;
      LOG_LEVEL = cfg.logLevel;
      LOG_FORMAT = cfg.logFormat;
    }
    // optionalAttrs (cfg.watchNamespace != null) { WATCH_NAMESPACE = cfg.watchNamespace; }
    // optionalAttrs (cfg.configFile != null) { SHINKA_CONFIG = toString cfg.configFile; }
    // optionalAttrs (cfg.discordWebhookUrl != null) { DISCORD_WEBHOOK_URL = cfg.discordWebhookUrl; }
    // cfg.extraEnv;
  in mkMerge [
    (mkIf (cfg.enable && isDarwin)
      (mkLaunchdService {
        name = "shinka";
        label = "io.pleme.shinka";
        command = "${cfg.package}/bin/shinka";
        inherit env;
        logDir = "${config.home.homeDirectory}/Library/Logs";
      }))

    (mkIf (cfg.enable && !isDarwin)
      (mkSystemdService {
        name = "shinka";
        description = "Shinka database migration operator";
        command = "${cfg.package}/bin/shinka";
        inherit env;
      }))
  ];
}
