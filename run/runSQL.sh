for i in ../lib/*; do
    CP=$CP:$i
done
"$JAVA_HOME/bin/java" -cp $CP:../dist/BenchmarkSQL-2.3.jar:../lib -Dprop=$1 -DcommandFile=$2 jdbc.ExecJDBC 
