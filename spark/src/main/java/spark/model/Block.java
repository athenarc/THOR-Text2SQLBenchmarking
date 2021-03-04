package spark.model;

import spark.SparkApplication;
import spark.components.BlockCreator;
import spark.components.BlockCreator.IndexArray;
import shared.util.Pair;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;

// This class models a Block (See spark paper page 7).
// A block is a combination of Strata objects. The Block
// holds a signature :
//   For each strata s in ST <Sum_s tf_wi(s), ...,  Sum_s tf_wm(s)>
// Its Signature is the aggregation of the Strata Signatures.
// The Block must have a tree like structure to represent the joins
// between the Strata. So it extends the JoiningNetworkOfTupleSets.
public class Block extends JoiningNetworkOfTupleSets {

    static public class ScoreComparator implements Comparator<Block> {
        @Override
        public int compare(Block a, Block b) {
            return b.score.compareTo(a.score);
        }
    }

    private Signature signature; // The blocks signature.
    private ScoreType status; // What scoring algorithm are we going to use for calculating this block's score.
    private Double score; // The block's score depending on the above algorithm type.

    private BlockCreator blockCreator; // The block creator instance that created this block.

    // Stores pairs of Integer, Nodes in the tree. The integer indicates
    // the stratum used as a tupleSet for the specific Node in the tree.
    List<Pair<Integer, Node>> stratumUsedPerNode;

    public Block(JoiningNetworkOfTupleSets networkToCopy) {
        super(networkToCopy);
        this.stratumUsedPerNode = new ArrayList<>();
        this.score = 0.0;
        this.status = null;
        this.signature = new Signature();
    }

    // Getters and Setters.
    public Signature getSignature() {
        return this.signature;
    }

    public ScoreType getStatus() {
        return this.status;
    }

    public void setStatus(ScoreType status) {
        this.status = status;
    }

    public Double getScore() {
        return this.score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public void setBlockCreator(BlockCreator blockCreator) {
        this.blockCreator = blockCreator;
    }

    public BlockCreator getBlockCreator() {
        return this.blockCreator;
    }

    public List<Pair<Integer, Node>> getStratumUsedPerNode() {
        return stratumUsedPerNode;
    }

    public void setStratumUsedPerNode(List<Pair<Integer, Node>> stratumUsedPerNode) {
        this.stratumUsedPerNode = stratumUsedPerNode;
    }

    // Calculates the block's signature by combining the signatures of its strata.
    public void computeSignature() {
        List<Signature> signatures = new ArrayList<>();

        // Loop through the list of pairs to get the signature of every stratum used to create the block.
        for (Pair<Integer, Node> pair :  this.stratumUsedPerNode) {
            if (pair.getRight().getTupleSet() instanceof Stratum) {
                signatures.add(((Stratum) pair.getRight().getTupleSet()).getSignature());
            }
        }

        // Create the block's signature by aggregating all the signatures from its strata components.
        this.signature.aggregateSignatures(signatures);

        // Update the block's set of keywords.
        super.setNetworkKeywords(this.signature.getContainedKeywords());
    }

    // Calculates and returns the uscore value (upper bound of the real score) of a block, according to Equation 4.
    public double computeUScore() {
        return this.computeUScoreA() * this.computeUScoreB() * super.sizeNormalizationFactor;
    }

    // Computes and returns the upper bound of the block's score_a value, according to Lemma 1.
    // Sums up the watf values of the block's strata signatures.
    private double computeUScoreA() {
        double s = 0.2; // Constant defined in the paper.
        double watfSum = 0.0; // Sum of the watf values of the block's strata signatures.

        for (Signature signature : this.blockCreator.getStrataSignatures(this)) {
            watfSum += signature.getWatf();
            // System.out.println("\tsignature: " + signature + " watf = " + signature.getWatf());
        }

        // System.out.println("\tsumidf = " + super.sumidf);

        // Since scores cannot be negative, we assign the maximum value to a negative score so it won't be considered.
        double a1 = 1 + Math.log(watfSum);
        double a2 = 1 + Math.log(a1);
        if ((a1 < 0) || (a2 < 0)) { a2 = Double.MAX_VALUE; }
        double A = super.sumidf * a2;
        double B = super.sumidf * watfSum;

        // System.out.println("\tA = " + A + "\n\tB = " + B);
        // System.out.println("\tuscore_a = " + (1.0 / (1.0 - s)) * Math.min(A, B));

        return ((1.0 / (1.0 - s)) * Math.min(A, B));
    }

    // Computes and returns the upper bound of the block's score_b value, according to the comment below Lemma 1.
    private double computeUScoreB() {
        // A constant value defined in the paper.
        // Switches the semantics from OR to AND when it increases to infinity. (A value of 2.0 is enough.)
        double p = 1.0;

        // The normalized term frequencies of all the keywords in the query, according to Equation 2.
        Map<String, Double> normalizedTermFrequencies = computeNormalizedTermFrequencies();

        double sum = 0.0; // The value of the summation in the formula.

        // Loop all keywords of the initial query to compute the summation.
        for (String keyword : Signature.getKeywords()) {
            sum += Math.pow(1.0 - normalizedTermFrequencies.get(keyword), p);
        }

        // Debug prints
        if (SparkApplication.DEBUG_PRINTS)
            System.out.println("\tuscore_b = " + (1.0 - Math.pow((sum / Signature.getKeywords().size()), (1.0 / p))));

        // Compute the final uscore_b value.
        return (1.0 - Math.pow((sum / Signature.getKeywords().size()), (1.0 / p)));
    }

    // Returns a map that associates all keywords of the query with their corresponding
    // normalized term frequency, according to Equation 2.
    // Only considers the keywords of the block to have a positive term frequency.
    private Map<String, Double> computeNormalizedTermFrequencies() {
        Map<String, Double> normalizedTermFrequencies = new HashMap<String, Double>();

        // Get the signatures of the block's strata.
        List<Signature> strataSignatures = this.blockCreator.getStrataSignatures(this);

        // Compute the maximum term frequency of all keywords for the block, by comparing the maximum of every stratum.
        int maximumKeywordFrequency = -1;
        for (Signature signature : strataSignatures) {
            int tempMax = signature.getMaximumFrequency();
            if (tempMax > maximumKeywordFrequency) {
                maximumKeywordFrequency = tempMax;
            }
        }

        // Compute the normalized term frequency of every keyword.
        for (String keyword : Signature.getKeywords()) {
            Double a = Double.valueOf(this.signature.getKeywordFrequency(keyword) / maximumKeywordFrequency);
            // System.out.println("idf " + super.getKeywordIdf(keyword) + " max " + super.getMaximumKeywordIdf());
            Double b = super.getKeywordIdf(keyword) / super.getMaximumKeywordIdf();
            normalizedTermFrequencies.put(keyword, a * b);
        }

        return normalizedTermFrequencies;
    }

    // Calculates and returns the bscore value (tighter upper bound of the real score) of a block, according to Equation 5.
    public double computeBScore() {        
        return this.computeBScoreSum() * this.computeScoreB() * super.sizeNormalizationFactor;
    }

    // Computes and returns the value of the summation in the formula (Equation 5).
    private double computeBScoreSum() {
        double sum = 0.0;
        double s = 0.2; // Constant defined in the paper.

        // Loop through the keywords of the block.
        for (String keyword : this.signature.getContainedKeywords()) {
            double a =  1 + Math.log(1 + Math.log(this.signature.getKeywordFrequency(keyword)));
            double b = Math.log(super.getKeywordIdf(keyword));
            sum += (a / (1.0 - s)) * b;
        }

        return sum;
    }

    // Computes and returns the score_b value of a block.
    public double computeScoreB() {
        // A constant value defined in the paper.
        // Switches the semantics from OR to AND when it increases to infinity. (A value of 2.0 is enough.)
        double p = 1.0;

        // The normalized term frequencies of all the keywords in the query, according to Equation 2.
        Map<String, Double> normalizedTermFrequencies = computeNormalizedTermFrequencies();

        double sum = 0.0; // The value of the summation in the formula.

        // Loop all keywords of the initial query to compute the summation.
        for (String keyword : Signature.getKeywords()) {
            sum += Math.pow(1.0 - normalizedTermFrequencies.get(keyword), p);
        }

        // Compute the final bscore value.
        return (1.0 - Math.pow((sum / Signature.getKeywords().size()), (1.0 / p)));
    }

    public void addStratumNodePair(Integer stratumIndex, Node node) {
        this.stratumUsedPerNode.add(new Pair<Integer, Node>(stratumIndex, node));
    }

    // Returns an array if integers indicating the indexes of the strata used to create this block.
    public IndexArray getStrataIndexesUsed() {
        int[] strataIndexes = new int[this.stratumUsedPerNode.size()];
        for (int i = 0; i < strataIndexes.length; i++) {
            strataIndexes[i] = this.stratumUsedPerNode.get(i).getLeft();
        }
        
        return new IndexArray(strataIndexes);
    }

    @Override
    public String toString() {
        String str = new String();
        str += "Block's Signature: " + this.signature.toString() + "\n" +
            "Block's Tree: " + super.debugPrint() + "\n";

        str += "Stratum Indexes: [";
        for (Pair<Integer, Node> pair :  this.stratumUsedPerNode) {
            str += pair.getRight().getJoinableExpressionsAbbreviation() + ": " + pair.getLeft() + ", ";
        }
        str = str.substring(0, str.length() - 2) + "]";

        return str;
    }

}
