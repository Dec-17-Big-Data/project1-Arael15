package com.revature.partition;

import java.util.*;

/**
 * This is a simple partitioner which simply divides the keys based upon which income group they fall into.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Q5Partitioner<K, V> extends Partitioner<Text, V> {
	
	private Set<String> lowIncome = new HashSet<>(Arrays.asList("Afghanistan", "Burundi", "Benin",
			"Burkina Faso", "Central African Republic", "Congo, Dem. Rep.", "Comoros", "Eritrea",
			"Ethiopia", "Guinea", "Gambia, The", "Guinea-Bissau", "Haiti", "Liberia", "Madagascar",
			"Mali", "Mozambique", "Malawi", "Niger", "Nepal", "Korea, Dem. People's Rep.", "Rwanda",
			"Senegal", "Sierra Leone", "Somalia", "South Sudan", "Chad", "Togo", "Tanzania", "Uganda", "Zimbabwe"));
	
	private Set<String> lowMidIncome = new HashSet<>(Arrays.asList("Armenia", "Bangladesh", "Bolivia",
			"Bhutan", "Côte d'Ivoire", "Cameroon", "Congo, Rep.", "Cabo Verde", "Djibouti", "Egypt, Arab Rep.",
			"Micronesia, Fed. Sts.", "Ghana", "Guatemala", "Honduras", "Indonesia", "India", "Kenya",
			"Kyrgyz Republic", "Cambodia", "Kiribati", "Lao PDR", "Sri Lanka", "Lesotho", "Morocco", "Moldova",
			"Myanmar", "Mongolia", "Mauritania", "Nigeria", "Nicaragua", "Pakistan", "Philippines", "Papua New Guinea",
			"West Bank and Gaza", "Sudan", "Solomon Islands", "El Salvador", "São Tomé and Principe", "Swaziland",
			"Syrian Arab Republic", "Tajikistan", "Timor-Leste", "Tonga", "Tunisia", "Ukraine", "Uzbekistan",
			"Vietnam", "Vanuatu", "Samoa", "Kosovo", "Yemen, Rep.", "Zambia"));
	
	private Set<String> upMidIncome = new HashSet<>(Arrays.asList("Angola", "Albania", "American Samoa",
			"Azerbaijan", "Bulgaria", "Bosnia and Herzegovina", "Belarus", "Belize", "Brazil", "Botswana",
			"China", "Colombia", "Costa Rica", "Cuba", "Dominica", "Dominican Republic", "Algeria", "Ecuador",
			"Fiji", "Gabon", "Georgia", "Equatorial Guinea", "Grenada", "Guyana", "Iran, Islamic Rep.", "Iraq",
			"Jamaica", "Jordan", "Kazakhstan", "Lebanon", "Libya", "St. Lucia", "Maldives", "Mexico",
			"Marshall Islands", "Macedonia, FYR", "Montenegro", "Mauritius", "Malaysia", "Namibia", "Panama",
			"Peru", "Palau", "Paraguay", "Romania", "Russian Federation", "Serbia", "Suriname", "Thailand",
			"Turkmenistan", "Turkey", "Tuvalu", "St. Vincent and the Grenadines", "Venezuela, RB", "South Africa"));
	
	private Set<String> highIncome = new HashSet<>(Arrays.asList("Aruba", "Andorra", "United Arab Emirates",
			"Antigua and Barbuda", "Australia", "Austria", "Belgium", "Bahrain", "Bahamas, The", "Bermuda",
			"Barbados", "Brunei Darussalam", "Canada", "Switzerland", "Channel Islands", "Chile", "Curaçao",
			"Cayman Islands", "Cyprus", "Czech Republic", "Germany", "Denmark", "Spain", "Estonia", "Finland",
			"France", "Faroe Islands", "United Kingdom", "Gibraltar", "Greece", "Greenland", "Guam",
			"Hong Kong SAR, China", "Croatia", "Hungary", "Isle of Man", "Ireland", "Iceland", "Israel",
			"Italy", "Japan", "St. Kitts and Nevis", "Korea, Rep.", "Kuwait", "Liechtenstein", "Lithuania",
			"Luxembourg", "Latvia", "Macao SAR, China", "St. Martin (French part)", "Monaco", "Malta",
			"Northern Mariana Islands", "New Caledonia", "Netherlands", "Norway", "Nauru", "New Zealand",
			"Oman", "Poland", "Puerto Rico", "Portugal", "French Polynesia", "Qatar", "Saudi Arabia",
			"Singapore", "San Marino", "Slovak Republic", "Slovenia", "Sweden", "Sint Maarten (Dutch part)",
			"Seychelles", "Turks and Caicos Islands", "Trinidad and Tobago", "Uruguay", "United States",
			"British Virgin Islands", "Virgin Islands (U.S.)"));

	@Override
	public int getPartition(Text key, V value, int numPartitions) {
		if (lowIncome.contains(key.toString())){
			return 0;
		}
		else if (lowMidIncome.contains(key.toString())){
			return 1;
		}
		else if (upMidIncome.contains(key.toString())){
			return 2;
		}
		else if (highIncome.contains(key.toString())){
			return 3;
		}
		else {
			return 4;
		}
	}

}
