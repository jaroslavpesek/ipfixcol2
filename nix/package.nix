{
  stdenv,
  lib,
  cmake,
  pkg-config,
  libfds,
  docutils,
  libxml2,
  rdkafka,
  zlib,
  lz4,
  nemea-framework,
  git,
  cacert,
  protobuf,
  xxHash
}:

stdenv.mkDerivation rec {
  pname = "ipfixcol2";
  version = "2.7.1";

  src = ../.;

  nativeBuildInputs = [ cmake pkg-config git cacert ];
  buildInputs = [ libfds docutils libxml2 rdkafka zlib lz4 nemea-framework protobuf xxHash ];

  postInstall = ''
    srcRoot=$(cd .. && pwd)
    for plugin in unirec clickhouse protobuf-kafka; do
      echo "Building $plugin plugin..."
      cd "$srcRoot/extra_plugins/output/$plugin"
      mkdir -p build && cd build
      cmake .. \
        -DCMAKE_INSTALL_PREFIX=$out \
        -DCMAKE_C_FLAGS="$CFLAGS -Wno-error=implicit-function-declaration" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
      make && make install
    done
  '';

  meta = {
    description = "Flexible, high-performance NetFlow v5/v9 and IPFIX flow data collector designed to be extensible by plugins";
    homepage = "https://github.com/CESNET/ipfixcol2";
    license = lib.licenses.gpl2Plus;
    platforms = lib.platforms.linux;
  };
}
