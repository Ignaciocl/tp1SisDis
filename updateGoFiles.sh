#! /bin/bash


cd server && go get -u github.com/Ignaciocl/tp1SisdisCommons && cd ..
cd distributor && go get -u github.com/Ignaciocl/tp1SisdisCommons && cd ..
cd calculator && go get -u github.com/Ignaciocl/tp1SisdisCommons && cd ..
cd healthchecker && go get -u github.com/Ignaciocl/tp1SisdisCommons && cd ..

cd joiners && for d in ./* ; do (cd "$d" && go get -u github.com/Ignaciocl/tp1SisdisCommons); done
cd ../workers && for d in ./* ; do (cd "$d" && go get -u github.com/Ignaciocl/tp1SisdisCommons); done
cd ../accumulators && for d in ./* ; do (cd "$d" && go get -u github.com/Ignaciocl/tp1SisdisCommons); done

echo "finished"
