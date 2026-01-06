{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.ipfixcol2;
in
{
  options.services.ipfixcol2 = {
    enable = mkEnableOption "ipfixcol2 IPFIX collector service";

    package = mkOption {
      type = types.package;
      default = pkgs.ipfixcol2;
      defaultText = literalExpression "pkgs.ipfixcol2";
      description = "The ipfixcol2 package to use.";
    };

    configXml = mkOption {
      type = types.path;
      description = "Path to the XML configuration file for ipfixcol2.";
    };

    verbosity = mkOption {
      type = types.str;
      default = "";
      example = "-vv";
      description = "Verbosity level of ipfixcol2 (-v, -vv, -vvv).";
    };

    extraArgs = mkOption {
      type = types.listOf types.str;
      default = [];
      example = [ "-p" "/run/ipfixcol2.pid" ];
      description = "Extra command-line arguments to pass to ipfixcol2.";
    };

    user = mkOption {
      type = types.str;
      default = "ipfixcol2";
      description = "User account under which ipfixcol2 runs.";
    };

    group = mkOption {
      type = types.str;
      default = "ipfixcol2";
      description = "Group under which ipfixcol2 runs.";
    };
  };

  config = mkIf cfg.enable {
    users.users.${cfg.user} = mkIf (cfg.user == "ipfixcol2") {
      isSystemUser = true;
      group = cfg.group;
      description = "ipfixcol2 service user";
    };

    users.groups.${cfg.group} = mkIf (cfg.group == "ipfixcol2") {};

    environment.etc."ipfixcol2/${builtins.baseNameOf cfg.configXml}" = {
      source = cfg.configXml;
    };

    systemd.services.ipfixcol2 = {
      description = "ipfixcol2 IPFIX collector";
      after = [ "network.target" ];
      wantedBy = [ "multi-user.target" ];

      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        ExecStart = concatStringsSep " " ([
          "${cfg.package}/bin/ipfixcol2"
          "-c" "/etc/ipfixcol2/${builtins.baseNameOf cfg.configXml}"
        ] ++ optional (cfg.verbosity != "") cfg.verbosity
          ++ cfg.extraArgs);
        Restart = "always";
        RestartSec = "5s";

        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
      };
    };
  };
}
