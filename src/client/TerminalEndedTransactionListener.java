package client;

/**
 * Created by IntelliJ IDEA.
 * User: rmpvilaca
 * Date: 11/08/04
 * Time: 17:33
 * To change this template use File | Settings | File Templates.
 */
public interface TerminalEndedTransactionListener {
    void signalTerminalEndedTransaction(String terminalName, String transactionType, long executionTime, String comment, int newOrder);

    void signalTerminalEnded(jTPCCTerminal jTPCCTerminal, long newOrderCounter);
}
