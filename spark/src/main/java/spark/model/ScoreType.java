package spark.model;

// Models the types of scores a block can have to indicate
// whether it is an upper bound or the real score.
public enum ScoreType {
    BSCORE,
    USCORE,
    SCORE // The real score
};
