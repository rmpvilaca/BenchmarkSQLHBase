CP=../lib
for i in ../lib/*; do
    CP=$CP:$i
done


$JAVA_HOME/bin/java -cp $CP:../dist/BenchmarkSQL-2.3.jar -Dprop=$1 client.jTPCCConsole {$2..$#}

