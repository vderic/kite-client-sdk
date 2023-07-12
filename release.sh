if [ $# -eq 0 ]; then
    echo "usage: $0 version [branch]"; echo
    exit 1
fi

BRANCH="main"
VERSION=$1
KITESDK=kite-client-sdk-$VERSION
if [ $# -eq 2 ]; then
BRANCH=$2
echo "BRANCH=$BRANCH"
fi

echo $KITESDK
rm -rf $KITESDK $KITESDK.zip
git clone git@github.com:vderic/kite-client-sdk.git $KITESDK
(cd $KITESDK && git checkout $BRANCH )
find $KITESDK -name '*.xrg' -delete
find $KITESDK -name '*.csv' -delete
find $KITESDK -name '*.csv.gz' -delete
rm -rf $KITESDK/.git*
zip -r $KITESDK.zip $KITESDK
rm -rf $KITESDK

