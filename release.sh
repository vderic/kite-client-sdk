if [ -z ${1+x} ]; then
    echo "usage: $0 version"; echo
    exit 1
fi

VERSION=$1
KITESDK=kite-client-sdk-$VERSION
echo $KITESDK
rm -rf $KITESDK $EXPREVAL.zip
git clone git@github.com:vderic/kite-client-sdk.git $KITESDK
find $KITESDK -name '*.xrg' -delete
find $KITESDK -name '*.csv' -delete
find $KITESDK -name '*.csv.gz' -delete
rm -rf $KITESDK/.git*
zip -r $KITESDK.zip $KITESDK
rm -rf $KITESDK

