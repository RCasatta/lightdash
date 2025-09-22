{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };
  outputs = { self, nixpkgs, flake-utils, rust-overlay, crane }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          inherit (pkgs) lib;

          rustToolchain = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

          # When filtering sources, we want to allow assets other than .rs files
          src = lib.cleanSourceWith {
            src = ./.; # The original, unfiltered source
            filter = path: type:
              # (lib.hasSuffix "\.elf" path) ||
              # (lib.hasInfix "/test_data/" path) ||

              # Default filter from crane (allow .rs files)
              (craneLib.filterCargoSources path type)
            ;
          };

          nativeBuildInputs = with pkgs; [ rustToolchain ]; # required only at build time
          buildInputs = [ ]; # also required at runtime

          commonArgs = {
            inherit src buildInputs nativeBuildInputs;

            # the following must be kept in sync with the ones in ./lwk_cli/Cargo.toml
            # note there should be a way to read those from there with
            # craneLib.crateNameFromCargoToml { cargoToml = ./path/to/Cargo.toml; }
            # but I can't make it work
            # pname = "lwk_cli";
            # version = "0.7.0";
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          bin = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
          });

        in
        {
          packages =
            {
              # that way we can build `bin` specifically,
              # but it's also the default.
              inherit bin;
              default = bin;
            };

          devShells.default = pkgs.mkShell {
            inputsFrom = [ bin ];

            buildInputs = [ rustToolchain pkgs.miniserve pkgs.just ];
          };
        }
      );
}

