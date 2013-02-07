CP=../lib
for i in ../lib/*; do
    CP=$CP:$i
done

PROP=$1
shift
$JAVA_HOME/bin/java -cp $CP:../dist/BenchmarkSQL-2.3.jar -Dprop=$PROP client.jTPCCConsole  $@

