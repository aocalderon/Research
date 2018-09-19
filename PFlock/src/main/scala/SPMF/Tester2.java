package SPMF;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/* This file is copyright (c) 2012-2014 Alan Souza
         *
         * This file is part of the SPMF DATA MINING SOFTWARE
         * (http://www.philippe-fournier-viger.com/spmf).
         *
         * SPMF is free software: you can redistribute it and/or modify it under the
         * terms of the GNU General Public License as published by the Free Software
         * Foundation, either version 3 of the License, or (at your option) any later
         * version.
         * SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
         * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
         * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
         * You should have received a copy of the GNU General Public License along with
         * SPMF. If not, see <http://www.gnu.org/licenses/>.
         */

/**
 * Example of how to use LCM algorithm from the source code.
 *
 * @author Alan Souza <apsouza@inf.ufrgs.br>
 */
public class Tester2 {
    public void runTests(String input) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(input));
        String line;
        // for each line (transaction) until the end of file
        List<List<Integer>> transactions = new ArrayList<>();
        HashSet<List<Integer>> transactionsSet = new HashSet<>();
        while (((line = reader.readLine()) != null)) {
            // split the line into items
            String[] lineSplited = line.split(" ");
            // for each item
            List<Integer> transaction = new ArrayList<>();
            for (String itemString : lineSplited) {
                // increase the support count of the item
                Integer item = Integer.parseInt(itemString);
                transaction.add(item);
            }
            // increase the transaction count
            transactions.add(transaction);
            transactionsSet.add(transaction);
        }
        int minsup = 1;
        int mu = 3;
        // Applying the algorithm
        AlgoFPMax fpMax2 = new AlgoFPMax();
        Itemsets itemsets = fpMax2.runAlgorithm(transactions, minsup);
        ArrayList<ArrayList<Integer>> maximal1 = itemsets.getItemsets(mu);
        fpMax2.printStats();

        AlgoLCM lcm = new AlgoLCM();
        Transactions data = new Transactions(transactionsSet);
        itemsets = lcm.runAlgorithm(minsup, data);
        ArrayList<ArrayList<Integer>> maximal2 = itemsets.getItemsets(mu);
        lcm.printStats();

        ArrayList<String> fpmaxStrings = new ArrayList<>();
        for (ArrayList<Integer> m: maximal1) {
            StringBuffer pattern = new StringBuffer();
            for(Integer i: m){
                pattern.append(i + " ");
            }
            fpmaxStrings.add(pattern.toString().trim());
        }
        Collections.sort(fpmaxStrings);

        ArrayList<String> lcmStrings = new ArrayList<>();
        for (ArrayList<Integer> m: maximal1) {
            StringBuffer pattern = new StringBuffer();
            for(Integer i: m){
                pattern.append(i + " ");
            }
            lcmStrings.add(pattern.toString().trim());
        }
        Collections.sort(lcmStrings);

        int n = lcmStrings.size();
        boolean identical = true;
        for(int i = 0; i < n; i++){
            if(lcmStrings.get(i).compareTo(fpmaxStrings.get(i)) != 0){
                identical = false;
                break;
            }
        }
        if(identical){
            System.out.println("\n");
            System.out.println("============================");
            System.out.println("  Results are identical!!!  ");
            System.out.println("============================");
            System.out.println("\n");
        }
    }

    public static void main(String[] arg) throws IOException {
        Tester2 test = new Tester2();
        int n = 155;

        for(int i = 0; i <= n; i++) {
            String input = "/home/and/Documents/PhD/Research/Validation/LCM_max/input/LCMinput_" + i + ".txt";
            System.out.println("\nRunning test with LCMinput_" + i + ".txt\n");
            test.runTests(input);
        }
    }
}
