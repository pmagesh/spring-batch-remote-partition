package transaction.domain;

import org.springframework.batch.item.ItemWriter;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;


public class TransactionWriter implements ItemWriter<Transaction> {

    private String outputFile;

    @Override
    public void write(List<? extends Transaction> items) throws Exception {
        List<String> enrichedTxnList = new ArrayList<>();
        items.forEach(item -> {
            String enrichedTxn = String.join(",", item.getTransactionId(), item.getMerchantId(), item.getMerchantName(), item.getTransactionAmt());
            enrichedTxnList.add(enrichedTxn);
        });
        enrichedTxnList.forEach(System.out::println);
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }
}
