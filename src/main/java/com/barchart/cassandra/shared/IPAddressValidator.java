package com.barchart.cassandra.shared;

public class IPAddressValidator {

	public static int countOccurrences(String haystack, char needle)
	{
	    int count = 0;
	    for (int i=0; i < haystack.length(); i++)
	        if (haystack.charAt(i) == needle)
	             count++;

	    return count;
	}

	/**
	 * Validate ip address
	 * 
	 * @param ip
	 *            ip address for validation
	 * @return true valid ip address, false invalid ip address
	 */
	static public boolean validate(final String ip) {

		return countOccurrences( ip, '.' ) % 3 == 0;
	}
}