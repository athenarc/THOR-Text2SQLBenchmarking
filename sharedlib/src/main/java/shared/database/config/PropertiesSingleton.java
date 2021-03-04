package shared.database.config;

import java.util.Enumeration;
import java.util.Properties;
import java.util.ResourceBundle;

/*
 * A singleton class that provides the application with resources
 * like database connectivity properties. Those resources should be palaced in a '.properties' file.
 */
public class PropertiesSingleton {

    private static ResourceBundle resource = null;

    /**
     * Lazy load the ResourceBundle using the parameter resourcePath
     * 
     * @param resourcePath
     */
    public static void loadPropertiesFile(String resourcePath) {
        // Lazy load the Singleton Class
        if (resource == null)
            resource = ResourceBundle.getBundle(resourcePath);
    }


    /**
     * Return the resource bundle if loaded 
     * 
     * @return
     */
    public static ResourceBundle getBundle() {
        return resource;
    }

    /**
     * From ResourceBundle to Properties
     */
    public static Properties convertResourceBundleToProperties(ResourceBundle resource) {
        Properties properties = new Properties();
        Enumeration<String> keys = resource.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            properties.put(key, resource.getString(key));
        }
        return properties;
    }
    
}
