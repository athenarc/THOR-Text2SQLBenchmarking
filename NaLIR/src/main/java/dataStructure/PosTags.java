package dataStructure;

import java.util.HashMap;

public class PosTags {

    public final static HashMap<String, String> map = new HashMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {
            put( "CC", "Coordinating conjunction");
            put( "CD", "Cardinal number");
            put( "DT", "Determiner");
            put( "EX", "Existential there");
            put( "FW", "Foreign word");
            put( "IN", "Preposition or subordinating conjunction");
            put( "JJ", "Adjective");
            put( "JJR", "Adjective, comparative");
            put( "JJS", "Adjective, superlative");
            put( "LS", "List item marker");
            put( "MD", "Modal");
            put( "NN", "Noun, singular or mass");
            put( "NNS", "Noun, plural");
            put( "NNP", "Proper noun, singular");
            put( "NNPS", "Proper noun, plural");
            put( "PDT", "Predeterminer");
            put( "POS", "Possessive ending");
            put( "PRP", "Personal pronoun");
            put( "PRP$", "Possessive pronoun");
            put( "RB", "Adverb");
            put( "RBR", "Adverb, comparative");
            put( "RBS", "Adverb, superlative");
            put( "RP", "Particle");
            put( "SYM", "Symbol");
            put( "TO", "to");
            put( "UH", "Interjection");
            put( "VB", "Verb, base form");
            put( "VBD", "Verb, past tense");
            put( "VBG", "Verb, gerund or present participle");
            put( "VBN", "Verb, past participle");
            put( "VBP", "Verb, non-3rd person singular present");
            put( "VBZ", "Verb, 3rd person singular present");
            put( "WDT", "Wh-determiner");
            put( "WP", "Wh-pronoun");
            put( "WP$", "Possessive wh-pronoun");
            put( "WRB", "Wh-adverb");
        }
    };

}