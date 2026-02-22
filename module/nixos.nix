# Shinka NixOS module — system-level service
#
# Namespace: services.shinka.*
{ config, lib, pkgs, ... }:
with lib; let
  cfg = config.services.shinka;
in {
  options.services.shinka = {
    enable = mkEnableOption "Shinka database migration operator";

    package = mkPackageOption pkgs "shinka" {};

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

  config = mkIf cfg.enable {
    systemd.services.shinka = {
      description = "Shinka database migration operator";
      after = ["network.target"];
      wantedBy = ["multi-user.target"];
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/shinka";
        DynamicUser = true;
        Restart = "on-failure";
        RestartSec = 5;
        ProtectSystem = "strict";
        ProtectHome = true;
        NoNewPrivileges = true;
      };
      environment = {
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
    };
  };
}
