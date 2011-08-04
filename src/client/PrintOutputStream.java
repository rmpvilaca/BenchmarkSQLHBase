package client;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * Created by IntelliJ IDEA.
 * User: rmpvilaca
 * Date: 11/08/04
 * Time: 17:24
 * To change this template use File | Settings | File Templates.
 */
public class PrintOutputStream implements PrintOutput{

    private PrintStream printStream;

    public PrintOutputStream(PrintStream printStream)
    {
        this.printStream=printStream;
    }

    public void print(String text) {
        printStream.print(text);
    }

    public void println(String text) {
        printStream.println(text);
    }

    public void clear() {
        System.out.println("clear");
    }
}
