package shared.connectivity.thor.response;

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * Based on the Paper "...", the General Architecture of an NL to SQL System consists of 4 components:
 *  - Parser: Parses the input user query and produces a structured representation out of it (eg ParseTree)
 *  - Entity Mapper: Find the entities refered by the user in the query in the underlying database.
 *  - Interpretation Generator: Generate the query from the system perspective, like a query graph containing joins and filters.
 *  - SQL Translator & Executor: Translates the above interpretations in SQL queries and executes them against an RDBMS.
 * 
 * Each of the above outputs a number of structures, by setting the information bellow THOR 
 * will be able to display and compare the database search System equally:
 *  - Parser outputs different (or even one) possible Parses (like parse Trees). Set the number of parses.
 *  - Entity Mapper outputs Database Mappings per Term found in the user query. Set the number of Relations where all keywords were found.
 *  - Interpretation Generator outputs structured representation of the query from the System/Db perspective, capturing joins/filters/... . Set the number of those representations
 *  - For the SQL Translator&Executor component simply set the number of SQL queries created and issued to the RDBMS.
 * </pre>
 */
public class GeneralArchitecture {

    // public Component parser = new Component("Parser");
    // public Component mapper = new Component("EntityMapper");
    // public Component irGenerator = new Component("Interpretation Generator");
    // public Component sqlTransAndExec = new Component("SQL Translator & Executor");

    /** 
     * This class represents the output of a General component of the system.
     */
    public class ComponentOutput {

        private String componentName; // The name of the component.
        private Integer output;

        /**
         * Constructor.
         */
        public ComponentOutput(String componentName, Integer output) {
            this.componentName = componentName;
            this.output = output;
        }

        /**
         * @return The name of the component.
         */
        public String getComponentName() {
            return this.componentName;
        }

        /**
         * @return The execution time of the component.
         */
        public Integer getOutput() {
            return this.output;
        }

        /**
         * @param output the output to set
         */
        public void setOutput(Integer output) {
            this.output = output;
        }

    }
    
    List<ComponentOutput> componentsOutput;  // The output of the general Components
    List<ComponentTime> componentsTime;      // The times of the general Components (if mapped by the user)
    // List<Component> mappedWithParser;
    // List<Component> mappedWithEntityMapper;
    // List<Component> mappedWithInterpretationGenerator;
    // List<Component> mappedWithTranslatorAndExecutor;

    
    public GeneralArchitecture() {
        this.componentsOutput = new ArrayList<>();
        this.componentsOutput.add(new ComponentOutput("Parser", 0));
        this.componentsOutput.add(new ComponentOutput("EntityMapper", 0));
        this.componentsOutput.add(new ComponentOutput("InterpretationGenerator", 0));
        this.componentsOutput.add(new ComponentOutput("SQLTranslator&Executor", 0));

        this.componentsTime = new ArrayList<>();
        this.componentsTime.add(new ComponentTime("Parser", 0D));
        this.componentsTime.add(new ComponentTime("EntityMapper", 0D));
        this.componentsTime.add(new ComponentTime("InterpretationGenerator", 0D));
        this.componentsTime.add(new ComponentTime("SQLTranslator&Executor", 0D));
        
        // this.mappedWithParser = new ArrayList<>();
        // this.mappedWithEntityMapper = new ArrayList<>();
        // this.mappedWithInterpretationGenerator = new ArrayList<>();
        // this.mappedWithTranslatorAndExecutor = new ArrayList<>();
    }


    /**
     * @param parserOutput the parserOutput to set
     */
    public void setParserOutput(Integer parserOutput) {
        this.componentsOutput.get(0).setOutput(parserOutput);
    }

    /**
     * @param entityMapperOutput the entityMapperOutput to set
     */
    public void setEntityMapperOutput(Integer entityMapperOutput) {
        this.componentsOutput.get(1).setOutput(entityMapperOutput);
    }

    /**
     * @param interpretationGeneratorOutput the interpretationGeneratorOutput to set
     */
    public void setInterpretationGeneratorOutput(Integer interpretationGeneratorOutput) {
        this.componentsOutput.get(2).setOutput(interpretationGeneratorOutput);
    }

    /**
     * @param translatorAndExecutorOutput the translatorAndExecutorOutput to set
     */
    public void setTranslatorAndExecutorOutput(Integer translatorAndExecutorOutput) {
        this.componentsOutput.get(3).setOutput(translatorAndExecutorOutput);
    }


    /**
     * Maps the argument components with the GeneraComponent Parser
     * 
     * @param components the components to map with parser
     */
    public void mapWithParser(Component... components) {
        Double time = 0D;
        for (Component component : components) {
            // this.mappedWithParser.add(component);
            time += component.getTime();
        }
        this.componentsTime.get(0).setTime(time);;
    }


    /**
     * Maps the argument components with the GeneraComponent EntityMapper
     * 
     * @param components the components to map with EntityMapper
     */
    public void mapWithEntityMapper(Component... components) {
        Double time = 0D;
        for (Component component : components) {
            // this.mappedWithEntityMapper.add(component);
            time += component.getTime();
        }
        this.componentsTime.get(1).setTime(time);;
    }


    /**
     * Maps the argument components with the GeneraComponent InterpretationGenerator
     * 
     * @param components the components to map with InterpretationGenerator
     */
    public void mapWithInterpretationGenerator(Component... components) {
        Double time = 0D;
        for (Component component : components) {
            // this.mappedWithInterpretationGenerator.add(component);
            time += component.getTime();
        }
        this.componentsTime.get(2).setTime(time);;
    }

    /**
     * Maps the argument components with the GeneraComponent TranslatorAndExecutor
     * 
     * @param components the components to map with TranslatorAndExecutor
     */
    public void mapWithTranslatorAndExecutor(Component... components) {
        Double time = 0D;
        for (Component component : components) {
            // this.mappedWithTranslatorAndExecutor.add(component);
            time += component.getTime();
        }
        this.componentsTime.get(3).setTime(time);;
    }

    /**
     * @return True if all component times for the general architecture components are 0;  
     */
    public boolean areTimesEmpty() {
        Double sum = 0D;
        for (ComponentTime time : this.componentsTime)
            sum += time.getTime();
        return (sum == 0D) ? true : false;
    }

    /**
     * @return True if all component outputs for the general architecture components are 0;  
     */
    public boolean areOutputsEmpty() {
        Double sum = 0D;
        for (ComponentOutput output : this.componentsOutput)
            sum += output.getOutput();
        return (sum == 0D) ? true : false;
    }


    /**
     * @param componentsTime the componentTimes to set
     */
    public void setComponentsTime(List<ComponentTime> componentTimes) {
        this.componentsTime = componentTimes;
    }

    /**
     * @param componentsOutput the componentOutputs to set
     */
    public void setComponentsOutput(List<ComponentOutput> componentOutputs) {
        this.componentsOutput = componentOutputs;
    }
}