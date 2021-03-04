package spark.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// This class models the signature of a tuple.
// We define the signature of a tuple t in R^Q as an
// ordered sequence of term frequencies for all the query
// keywords:
//      <tf_w1(t), ..., tf_wm(t)>
public class Signature {

    // All the keywords of the query.
    // It is declared as static because all signature instances must have the same sequence of keywords.
    private static List<String> keywords;

    private int[] frequencies; // Contains the frequencies of the above keywords.
    private double watf; // The watf value of the signature, which is used to sort tuples or strata.

    public Signature() {
        this.frequencies = new int[Signature.keywords.size()]; // Initialized to zeros.
    }

    // Getters and Setters.
    public static List<String> getKeywords() {
        return Signature.keywords;
    }

    public static void setKeywords(List<String> keywords) {
        Signature.keywords = keywords;
    }

    public Double getWatf() {
        return this.watf;
    }

    // Returns a list of the keywords with a non-zero frequency.
    public List<String> getContainedKeywords() {
        List<String> containedKeywords = new ArrayList<>();
        for (int index = 0; index < this.frequencies.length; index++) {
            if (this.frequencies[index] > 0) {
                containedKeywords.add(Signature.keywords.get(index));
            }
        }

        return containedKeywords;
    }

    // Sets the frequency of a keyword.
    public void setFrequency(String keyword, int frequency) {
        try {
            this.frequencies[Signature.keywords.indexOf(keyword)] = frequency;
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.print("[WARN] In Signature.setFrequency() : Unknown Keyword \"" + keyword + "\" passed.");
        }
    }

    // Returns the frequency of a keyword.
    public int getKeywordFrequency(String keyword) {
        int frequency = 0;
        try {
            frequency = this.frequencies[Signature.keywords.indexOf(keyword)];
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.print("[WARN] In Signature.setFrequency() : Unknown Keyword \"" + keyword + "\" passed.");
        }

        return frequency;
    }

    // Returns the maximum value among all frequencies.
    public int getMaximumFrequency() {
        int max = -1;
        for (int i = 0; i < this.frequencies.length; i++) {
            if (this.frequencies[i] > max) {
                max = this.frequencies[i];
            }
        }

        return max;
    }

    // Given a map of strings and their occurrences, this function fills the signature's frequencies array.
    public void fill(Map<String, Integer> keywordFrequencies) {
        for (Map.Entry<String, Integer> entry : keywordFrequencies.entrySet()) {
            setFrequency(entry.getKey(), entry.getValue());
        }
    }

    // Creates a new signature (this object) by combining the list of signatures passed as argument.
    public void aggregateSignatures(List<Signature> signatures) {
        for (Signature signature : signatures) {
            // The lengths must be the same.
            if (signature.frequencies.length != this.frequencies.length) {
                System.err.println("[WARN] In Signature.aggregateSignatures() : Signatures with different size.");
                continue;
            }

            // Loop the signature's frequency array and add the frequencies of the arguments.
            for (int index = 0; index < this.frequencies.length; index++) {
                this.frequencies[index] += signature.frequencies[index];
            }
        }
    }

    // Increments the frequency of a keyword.
    public void incrementFrequency(String keyword) {
        try {
            this.frequencies[Signature.keywords.indexOf(keyword)] += 1;
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.print("[WARN] In Signature.incrementFrequency() : Unknown Keyword \"" + keyword + "\" passed.");
        }
    }

    // Computes and stores the watf value of the signature.
    public void computeWatf(JoiningNetworkOfTupleSets network) {
        double sum = 0.0;
        
        // Compute the summation of the numerator.
        for (String keyword : Signature.keywords) {
            int keywordFrequency = this.frequencies[Signature.keywords.indexOf(keyword)];
            if (keywordFrequency > 0) {
                sum += keywordFrequency * network.getKeywordIdf(keyword);
            }
        }

        // Compute and save the final watf value.
        this.watf = sum / network.getSumidf();
    }

    @Override
    public int hashCode() {
        int hash = 31;
        for (int index = 0; index < this.frequencies.length; index++) {
            // Only consider the positive frequency values.
            if (this.frequencies[index] > 0) {
                hash += hash * index * this.frequencies[index];
            }
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null) return false;
        if (!(obj instanceof Signature)) return false;
        Signature sig = (Signature) obj;

        // The signatures are equal if the frequencies are equal.
        if (sig.frequencies.length != this.frequencies.length) return false;
        for (int index = 0; index < this.frequencies.length; index++) {
            if (sig.frequencies[index] != this.frequencies[index])
                return false;
        }

        return true;
    }

    @Override
    public String toString() {
        String str = new String();
        str += "<";
        for (int index = 0; index < this.frequencies.length; index++) {
            str += Signature.keywords.get(index) + ": " + this.frequencies[index] + ", ";
        }
        str = str.substring(0, str.length() - 2) + ">";
        return str;
    }

}
