package com.ctrip.hermes.rest.common;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class Configuration {
    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);
    /**
     * List of default Resources. Resources are loaded in the order of the list entries
     */
    private static List<String> defaultResources =
            new CopyOnWriteArrayList<String>();
    private static ClassLoader classLoader;
    private static Map<String, String> configMap =
            new HashMap<String, String>();
    private static ArrayList<String> configFiles = new ArrayList<String>();

    static {
        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = Configuration.class.getClassLoader();
        }
        loadDefaultConfig();
    }

    public static Map<String, String> getAllConfig() {
        return ImmutableMap.copyOf(configMap);
    }

    public static void addResource(String name) {
        loadConfig(name);
    }

    /**
     * Add a default resource. Resources are loaded in the order of the resources
     * added.
     *
     * @param name file name. File should be present in the classpath.
     */
    public static synchronized void addDefaultResource(String name) {
        if (!defaultResources.contains(name)) {
            defaultResources.add(name);
        }
    }

    private static void loadDefaultConfig() {
        for (String defaultResource : defaultResources) {
            loadConfig(defaultResource);
        }
    }

    @SuppressWarnings("unchecked")
    private static void loadConfig(String confFile) {
        Properties props = new Properties();
        InputStream in = null;
        try {
            URL url = classLoader.getResource(confFile);
            if (url == null) {
                return;
            }

            in = url.openStream();
            props.load(in);
            Enumeration<String> en = (Enumeration<String>) props.propertyNames();
            while (en.hasMoreElements()) {
                String key = en.nextElement();
                configMap.put(key, props.getProperty(key));
            }
            // Only add existed file into configFiles
            configFiles.add(confFile);
        } catch (Exception e) {
            LOGGER.warn("Cannot load configuration from file <" + confFile + ">", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * Get the value of the name property
     *
     * @param name the property name.
     * @return the value of the name, or null if no such property exists.
     */
    public static String get(String name) {
        return getTrimmed(name);
    }

    /**
     * Get the value of the name property
     *
     * @param name         the property name.
     * @param defaultValue default value.
     * @return the value of the name, or defaultValue if no such property exists.
     */
    public static String get(String name, String defaultValue) {
        String result = get(name);
        if (result == null) {
            result = defaultValue;
        }
        return result;
    }

    /**
     * Get the value of the name property as a trimmed string.
     *
     * @param name the property name.
     * @return the value of the name, or defaultValue if no such property exists.
     */
    private static String getTrimmed(String name) {
        String value = configMap.get(name);
        if (null == value) {
            return null;
        } else {
            return value.trim();
        }
    }

    /**
     * Get the value of the name property
     *
     * @param name         the property name.
     * @param defaultValue default value.
     * @return the value of the name, or defaultValue if no such property exists.
     */
    public static int getInt(String name, int defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }
        return Integer.parseInt(valueString);
    }

    public static int getInt(String name) {
        return Integer.parseInt(get(name));
    }

    public static boolean getBoolean(String name, boolean defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }
        return Boolean.valueOf(valueString);
    }

    public static boolean getBoolean(String name) {
        return Boolean.valueOf(get(name));
    }

    public static String[] getStrings(String name) {
        String valueString = get(name);
        return getTrimmedStrings(valueString);
    }

    private static String[] getTrimmedStrings(String str) {
        String[] emptyStringArray = {};
        if (null == str || "".equals(str.trim())) {
            return emptyStringArray;
        }

        return str.trim().split("\\s*,\\s*");
    }

    public static Class<?> getClass(String name) throws ClassNotFoundException {
        String valueString = getTrimmed(name);
        if (valueString == null) {
            throw new ClassNotFoundException("Class " + name + " not found");
        }
        return Class.forName(valueString, true, classLoader);
    }

    public static Class<?>[] getClasses(String name) throws ClassNotFoundException {
        String[] classNames = getStrings(name);
        if (classNames == null) {
            return null;
        }
        Class<?>[] classes = new Class<?>[classNames.length];
        for (int i = 0; i < classNames.length; i++) {
            classes[i] = getClass(classNames[i]);
        }
        return classes;
    }


    public static void dumpDeprecatedKeys() {
        for (String key : configMap.keySet()) {
            System.out.println(key + "=" + configMap.get(key));
        }
    }
    
    // Change configuration at runtime, e.g.: for the convenience of test case mockup.
    public static void set(String key, String value) {
    	if (StringUtils.isBlank(key))
    		throw new IllegalArgumentException("Key [" + key + "] is blank, invalid");
    	
    	if (StringUtils.isBlank(value))
    		throw new IllegalArgumentException("Value [" + value + "] is blank, invalid");
    		
    	configMap.put(key, value);
    }

    public static void set(String key, String value, boolean isWriterToFile) {
        if(isWriterToFile) {
            String fileToWrite = null;
            // if only contains "xx-default.properties"
            if(configFiles.size() == 1) {
                fileToWrite = configFiles.get(0);
            } else {
                // not write into "xx-default.properties"
                for(String config : configFiles) {
                    if(! config.contains("default"))
                        fileToWrite = config;
                }
            }
            //Now, get the file to write, then write.
            InputStream in = null;
            Properties props = new Properties();
            try {
                URL url = classLoader.getResource(fileToWrite);

                in = url.openStream();
                props.load(in);
                props.put(key, value);
                props.store(new FileOutputStream(new File(url.toURI())), null);
                in.close();
            }catch (IOException ioe) {
                LOGGER.error("Cannot Write configuration TO <" + fileToWrite + ">", ioe);
            } catch (URISyntaxException e) {
                LOGGER.error("Cannot Write configuration TO <" + fileToWrite + ">: URISyntaxException", e);
            }
        }
        set(key, value);
    }

    public static String getPropOrConfig(String key, String defaultValue) {
    	String value = System.getProperty(key);
    	if (StringUtils.isBlank(value)) {
    		value = get(key);
    		if (StringUtils.isBlank(value)) {
    			return defaultValue;
    		}
    	}
    	return value;
    }
}
