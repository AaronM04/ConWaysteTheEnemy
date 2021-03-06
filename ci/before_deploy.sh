# This script takes care of building your crate and packaging it for release

set -ex

main() {
    local src=$(pwd) \
          stage=

    case $TRAVIS_OS_NAME in
        linux)
            stage=$(mktemp -d)
            ;;
        osx)
            stage=$(mktemp -d -t tmp)
            ;;
    esac

    test -f Cargo.lock || cargo generate-lockfile

    # Update this to build the artifacts that matter to you
    cross rustc --bin client --target $TARGET --package conwayste --release -- -C lto

    # Update this to package the right artifacts
    if echo $TARGET | grep -q pc-windows; then
      EXT=.exe
    else
      EXT=
    fi
    cp target/$TARGET/release/client$EXT $stage/
    cp -pr conwayste/resources $stage/

    cd $stage
    tar czf $src/$CRATE_NAME-$TRAVIS_TAG-$TARGET.tar.gz *
    cd $src

    rm -rf $stage
}

main
