pip install --extra-index-url https://pypi.chia.net/simple/ bladebit

$pypath=(pip show bladebit | findstr "Location").split(" ")[1]
echo $pypath

cp -r $pypath/bladebit/lib libs
cp -r $pypath/bladebit/include src/bladebit

ls
ls libs
ls src/bladebit
