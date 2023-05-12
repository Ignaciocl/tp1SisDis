#! /bin/bash


cd server && go get -u github.com/Ignaciocl/tp1SisdisCommons && cd ..
cd distributor && go get -u github.com/Ignaciocl/tp1SisdisCommons && cd ..
cd manager && go get -u github.com/Ignaciocl/tp1SisdisCommons && cd ..

cd joiners && for d in ./* ; do (cd "$d" && go get -u github.com/Ignaciocl/tp1SisdisCommons); done
cd ../workers && for d in ./* ; do (cd "$d" && go get -u github.com/Ignaciocl/tp1SisdisCommons); done
cd ../accumulators && for d in ./* ; do (cd "$d" && go get -u github.com/Ignaciocl/tp1SisdisCommons); done

echo "finished"
