{
  description = "ipfixcol2 - IPFIX flow data collector";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    systems.url = "github:nix-systems/default";
    netmonpkgs.url = "github:jaroslavpesek/netmonpkgs";
    flake-utils = {
      url = "github:numtide/flake-utils";
      inputs.systems.follows = "systems";
    };
  };

  outputs = { self, nixpkgs, flake-utils, netmonpkgs, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        ipfixcol2 = pkgs.callPackage ./nix/package.nix {
          libfds = netmonpkgs.packages.${system}.libfds;
          nemea-framework = netmonpkgs.packages.${system}.nemea-framework;
        };
      in
      {
        packages = {
          default = ipfixcol2;
          ipfixcol2 = ipfixcol2;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ ipfixcol2 ];
          packages = [
            pkgs.bashInteractive
            pkgs.nixd
            pkgs.nixpkgs-fmt
            pkgs.gcc
          ];
          shellHook = ''
            echo "Welcome to ipfixcol2 development environment."
          '';
        };

        formatter = pkgs.nixpkgs-fmt;
      }
    ) // {
      overlays.default = final: prev: {
        ipfixcol2 = self.packages.${prev.system}.default;
      };

      nixosModules = {
        default = import ./nix/module.nix;
        ipfixcol2 = import ./nix/module.nix;
      };
    };
}