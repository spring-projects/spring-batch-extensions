/*
 * Copyright 2006-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.item.excel.mapping;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.excel.RowMapper;
import org.springframework.batch.item.excel.support.rowset.RowSet;
import org.springframework.batch.item.excel.support.rowset.RowSetMetaData;
import org.springframework.beans.*;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@link RowMapper} implementation that converts a row into a new instance
 * of the specified mapped target class. The mapped target class must be a
 * top-level class and it must have a default or no-arg constructor.
 *
 * Column values are mapped based on matching the column name as obtained from row set
 * metadata to public setters for the corresponding properties. The names are matched either
 * directly or by transforming a name separating the parts with underscores to the same name
 * using "camel" case.
 *
 * Mapping is provided for fields in the target class for many common types, e.g.:
 * String, boolean, Boolean, byte, Byte, short, Short, int, Integer, long, Long,
 * float, Float, double, Double, BigDecimal, {@code java.util.Date}, etc.
 *
 * For 'null' values read from the Excel document, we will attempt to call the setter, but in the case of
 * Java primitives, this causes a TypeMismatchException. This class can be configured (using the
 * primitivesDefaultedForNullValue property) to trap this exception and use the primitives default value.
 * Be aware that if you use the values from the generated bean to update the database the primitive value
 * will have been set to the primitive's default value instead of null.
 *
 * Please note that this class is designed to provide convenience rather than high performance.
 * For best performance, consider using a custom {@link RowMapper} implementation.
 *
 * @author Marten Deinum
 * @since 0.5.0
 */
public class BeanPropertyRowMapper<T> implements RowMapper<T>, BeanFactoryAware, InitializingBean {

    /**
     * Logger available to subclasses
     */
    protected final Log logger = LogFactory.getLog(getClass());

    /**
     * The class we are mapping to
     */
    private Class<T> type;

    /**
     * The name of the bean we are mapping to
     */
    private String name;

    /**
     * Whether we're strictly validating
     */
    private boolean checkFullyPopulated = false;

    /**
     * Whether we're defaulting primitives when mapping a null value
     */
    private boolean primitivesDefaultedForNullValue = false;

    /**
     * Map of the fields we provide mapping for
     */
    private Map<String, PropertyDescriptor> mappedFields;

    /**
     * Set of bean properties we provide mapping for
     */
    private Set<String> mappedProperties;

    private BeanFactory beanFactory;

    /**
     * Create a new BeanPropertyRowMapper for bean-style configuration.
     *
     * @see #setTargetType
     * @see #setPrototypeBeanName
     * @see #setCheckFullyPopulated
     */
    public BeanPropertyRowMapper() {
    }


    /**
     * The bean name (id) for an object that can be populated from the field set
     * that will be passed into {@link #mapRow(RowSet)}. Typically a
     * prototype scoped bean so that a new instance is returned for each field
     * set mapped.
     * <p/>
     * Either this property or the type property must be specified, but not
     * both.
     *
     * @param name the name of a prototype bean in the enclosing BeanFactory
     */
    public void setPrototypeBeanName(String name) {
        this.name = name;
    }

    /**
     * Public setter for the type of bean to create instead of using a prototype
     * bean. An object of this type will be created from its default constructor
     * for every call to {@link #mapRow(RowSet)}.<br>
     * <p/>
     * Either this property or the prototype bean name must be specified, but
     * not both.
     *
     * @param type the type to set
     */
    public void setTargetType(Class<T> type) {
        this.type = type;
    }

    /**
     * Initialize the mapping metadata for the given class.
     *
     * @param mappedClass the mapped class.
     */
    protected void initialize(Class<T> mappedClass) {
        this.mappedFields = new HashMap<String, PropertyDescriptor>();
        this.mappedProperties = new HashSet<String>();
        PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(mappedClass);
        for (PropertyDescriptor pd : pds) {
            if (pd.getWriteMethod() != null) {
                this.mappedFields.put(pd.getName().toLowerCase(), pd);
                String underscoredName = underscoreName(pd.getName());
                if (!pd.getName().toLowerCase().equals(underscoredName)) {
                    this.mappedFields.put(underscoredName, pd);
                }
                this.mappedProperties.add(pd.getName());
            }
        }
    }

    /**
     * Convert a name in camelCase to an underscored name in lower case.
     * Any upper case letters are converted to lower case with a preceding underscore.
     *
     * @param name the string containing original name
     * @return the converted name
     */
    private String underscoreName(String name) {
        if (!StringUtils.hasLength(name)) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        result.append(name.substring(0, 1).toLowerCase());
        for (int i = 1; i < name.length(); i++) {
            String s = name.substring(i, i + 1);
            String slc = s.toLowerCase();
            if (!s.equals(slc)) {
                result.append("_").append(slc);
            } else {
                result.append(s);
            }
        }
        return result.toString();
    }

    /**
     * Set whether we're strictly validating that all bean properties have been
     * mapped from corresponding database fields.
     * <p>Default is {@code false}, accepting unpopulated properties in the
     * target bean.
     */
    public void setCheckFullyPopulated(boolean checkFullyPopulated) {
        this.checkFullyPopulated = checkFullyPopulated;
    }

    /**
     * Return whether we're strictly validating that all bean properties have been
     * mapped from corresponding database fields.
     */
    public boolean isCheckFullyPopulated() {
        return this.checkFullyPopulated;
    }

    /**
     * Set whether we're defaulting Java primitives in the case of mapping a null value
     * from corresponding database fields.
     * <p>Default is {@code false}, throwing an exception when nulls are mapped to Java primitives.
     */
    public void setPrimitivesDefaultedForNullValue(boolean primitivesDefaultedForNullValue) {
        this.primitivesDefaultedForNullValue = primitivesDefaultedForNullValue;
    }

    /**
     * Return whether we're defaulting Java primitives in the case of mapping a null value
     * from corresponding database fields.
     */
    public boolean isPrimitivesDefaultedForNullValue() {
        return primitivesDefaultedForNullValue;
    }

    /**
     * Extract the values for all columns in the current row.
     * <p>Utilizes public setters and result set metadata.
     *
     * @see java.sql.ResultSetMetaData
     */
    @Override
    public T mapRow(RowSet rs) throws Exception {
        T mappedObject = getBean();
        BeanWrapper bw = PropertyAccessorFactory.forBeanPropertyAccess(mappedObject);
        initBeanWrapper(bw);

        RowSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        Set<String> populatedProperties = (isCheckFullyPopulated() ? new HashSet<String>() : null);

        for (int index = 0; index < columnCount; index++) {
            String column = rsmd.getColumnName(index);
            PropertyDescriptor pd = this.mappedFields.get(column.replaceAll(" ", "").toLowerCase());
            if (pd != null) {
                String value = rs.getColumnValue(index);
                if (logger.isDebugEnabled()) {
                    logger.debug("Mapping column '" + column + "' to property '" +
                            pd.getName() + "' of type " + pd.getPropertyType());
                }
                try {

                    bw.setPropertyValue(pd.getName(), value);
                } catch (TypeMismatchException e) {
                    if (value == null && primitivesDefaultedForNullValue) {
                        logger.debug("Intercepted TypeMismatchException for row " + rs.getCurrentRowIndex() +
                                " on sheet " + rsmd.getSheetName() + " and column '" + column + "' with value " + value +
                                " when setting property '" + pd.getName() + "' of type " + pd.getPropertyType() +
                                " on object: " + mappedObject);
                    } else {
                        throw e;
                    }
                }
                if (populatedProperties != null) {
                    populatedProperties.add(pd.getName());
                }
            }
        }

        if (populatedProperties != null && !populatedProperties.equals(this.mappedProperties)) {
            throw new IllegalStateException("Given RowSet does not contain all fields " +
                    "necessary to populate object of class [" + mappedObject.getClass() + "]: " + this.mappedProperties);
        }

        return mappedObject;
    }

    /**
     * Initialize the given BeanWrapper to be used for row mapping.
     * To be called for each row.
     * <p>The default implementation is empty. Can be overridden in subclasses.
     *
     * @param bw the BeanWrapper to initialize
     */
    protected void initBeanWrapper(BeanWrapper bw) {
    }

    private T getBean() {
        if (name != null) {
            return (T) beanFactory.getBean(name);
        }
        try {
            return type.newInstance();
        } catch (InstantiationException e) {
            ReflectionUtils.handleReflectionException(e);
        } catch (IllegalAccessException e) {
            ReflectionUtils.handleReflectionException(e);
        }
        // should not happen
        throw new IllegalStateException("Internal error: could not create bean instance for mapping.");
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.state(name != null || type != null, "Either name or type must be provided.");
        Assert.state(name == null || type == null, "Both name and type cannot be specified together.");
        initialize((Class<T>) getBean().getClass());
    }

    /**
     * Static factory method to create a new BeanPropertyRowMapper
     * (with the mapped class specified only once).
     *
     * @param targetType the class that each row should be mapped to
     */
    public static <T> BeanPropertyRowMapper<T> newInstance(Class<T> targetType) {
        BeanPropertyRowMapper<T> newInstance = new BeanPropertyRowMapper<T>();
        newInstance.setTargetType(targetType);
        return newInstance;
    }
}
