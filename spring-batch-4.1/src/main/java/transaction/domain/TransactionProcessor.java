package transaction.domain;

import org.springframework.batch.item.ItemProcessor;


public class TransactionProcessor implements ItemProcessor<Transaction, Transaction> {
    @Override
    public Transaction process(Transaction item) throws Exception {

        if ("1001".equalsIgnoreCase(item.getMerchantId())) {
            item.setMerchantName("Proposal");
        } else if ("1002".equalsIgnoreCase(item.getMerchantId())) {
            item.setMerchantName("Walmart");
        } else {
            item.setMerchantName("Not Available");
        }
        System.out.println("Enriched Trasaction Details --> " + item.toString());
        return item;
    }
}
