/*
 * Copyright 2006-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.extensions.excel.mapping;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.BeanUtils;
import org.springframework.util.StringUtils;

/**
 * Helper class for calculating bean property matches, according to. Used by
 * BeanWrapperImpl to suggest alternatives for an invalid property name.<br>
 *
 * Copied and slightly modified from Spring core,
 *
 * @author Alef Arendsen
 * @author Arjen Poutsma
 * @author Juergen Hoeller
 * @author Dave Syer
 * @since 0.1.0
 * @see #forProperty(String, Class, int)
 */
final class PropertyMatches {

	/**
	 * Create PropertyMatches for the given bean property.
	 * @param propertyName the name of the property to find possible matches for
	 * @param beanClass the bean class to search for matches
	 * @param maxDistance the maximum property distance allowed for matches
	 * @return the prepared {@code PropertyMatches}
	 */
	static PropertyMatches forProperty(String propertyName, Class<?> beanClass, int maxDistance) {
		return new PropertyMatches(propertyName, beanClass, maxDistance);
	}

	private final String propertyName;

	private final String[] possibleMatches;

	/**
	 * Create a new PropertyMatches instance for the given property.
	 * @param propertyName the name of the property to find possible matches for
	 * @param beanClass the bean class to search for matches
	 * @param maxDistance the maximum property distance allowed for matches
	 */
	private PropertyMatches(String propertyName, Class<?> beanClass, int maxDistance) {
		this.propertyName = propertyName;
		this.possibleMatches = calculateMatches(BeanUtils.getPropertyDescriptors(beanClass), maxDistance);
	}

	String[] getPossibleMatches() {
		return this.possibleMatches;
	}

	/**
	 * Generate possible property alternatives for the given property and class.
	 * Internally uses the <code>getStringDistance</code> method, which in turn uses the
	 * Levenshtein algorithm to determine the distance between two Strings.
	 * @param propertyDescriptors the JavaBeans property descriptors to search
	 * @param maxDistance the maximum distance to accept
	 * @return the calculated matches
	 */
	private String[] calculateMatches(PropertyDescriptor[] propertyDescriptors, int maxDistance) {
		List<String> candidates = new ArrayList<>();
		for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
			if (propertyDescriptor.getWriteMethod() != null) {
				String possibleAlternative = propertyDescriptor.getName();
				int distance = calculateStringDistance(this.propertyName, possibleAlternative);
				if (distance <= maxDistance) {
					candidates.add(possibleAlternative);
				}
			}
		}
		Collections.sort(candidates);
		return StringUtils.toStringArray(candidates);
	}

	/**
	 * Calculate the distance between the given two Strings according to the Levenshtein
	 * algorithm.
	 * @param s1 the first String
	 * @param s2 the second String
	 * @return the distance value
	 */
	private int calculateStringDistance(String s1, String s2) {
		if (s1.length() == 0) {
			return s2.length();
		}
		if (s2.length() == 0) {
			return s1.length();
		}
		int[][] d = new int[s1.length() + 1][s2.length() + 1];

		for (int i = 0; i <= s1.length(); i++) {
			d[i][0] = i;
		}
		for (int j = 0; j <= s2.length(); j++) {
			d[0][j] = j;
		}

		for (int i = 1; i <= s1.length(); i++) {
			char s_i = s1.charAt(i - 1);
			for (int j = 1; j <= s2.length(); j++) {
				int cost;
				char t_j = s2.charAt(j - 1);
				if (Character.toLowerCase(s_i) == Character.toLowerCase(t_j)) {
					cost = 0;
				}
				else {
					cost = 1;
				}
				d[i][j] = Math.min(Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1), d[i - 1][j - 1] + cost);
			}
		}

		return d[s1.length()][s2.length()];
	}

}
