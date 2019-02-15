package SPMF;

import java.util.List;

public class Call {
    private List[] buckets;
    private List<Integer> p;
    private List<Transaction> transactionsOfP;
    private List<Integer> frequentItems;
    private int tailPosInP;

    public Call(List<Integer> p, List<Transaction> transactionsOfP, List<Integer> frequentItems, int tailPosInP, List[] buckets) {
        this.p = p;
        this.transactionsOfP = transactionsOfP;
        this.frequentItems = frequentItems;
        this.tailPosInP = tailPosInP;
        this.buckets = buckets;
    }

    public List<Integer> getP() {
        return p;
    }

    public List<Transaction> getTransactionsOfP() {
        return transactionsOfP;
    }

    public List<Integer> getFrequentItems() {
        return frequentItems;
    }

    public int getTailPosInP() {
        return tailPosInP;
    }

    public List[] getBuckets() { return buckets; }
}
