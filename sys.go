package main

import "os"

var fileName = os.Args[1][:len(os.Args[1])-4]

func decodeYr(y string) string {
	yrDecDict := map[string]string{
		"0": "2000", "1": "2001", "2": "2002", "3": "2003", "4": "2004",
		"5": "2005", "6": "2006", "7": "2007", "8": "2008", "9": "2009",
		"10": "2010", "11": "2011", "12": "2012", "13": "2013", "14": "2014",
		"15": "2015", "16": "2016", "17": "2017", "18": "2018", "19": "2019",
		"20": "2020", "40": "1940", "41": "1941", "42": "1942", "43": "1943",
		"44": "1944", "45": "1945", "46": "1946", "47": "1947", "48": "1948",
		"49": "1949", "50": "1950", "51": "1951", "52": "1952", "53": "1953",
		"54": "1954", "55": "1955", "56": "1956", "57": "1957", "58": "1958",
		"59": "1959", "60": "1960", "61": "1961", "62": "1962", "63": "1963",
		"64": "1964", "65": "1965", "66": "1966", "67": "1967", "68": "1968",
		"69": "1969", "70": "1970", "71": "1971", "72": "1972", "73": "1973",
		"74": "1974", "75": "1975", "76": "1976", "77": "1977", "78": "1978",
		"79": "1979", "80": "1980", "81": "1981", "82": "1982", "83": "1983",
		"84": "1984", "85": "1985", "86": "1986", "87": "1987", "88": "1988",
		"89": "1989", "90": "1990", "91": "1991", "92": "1992", "93": "1993",
		"94": "1994", "95": "1995", "96": "1996", "97": "1997", "98": "1998",
		"99": "1999",
	}
	if dy, ok := yrDecDict[y]; ok {
		return dy
	}
	return y
}

func decAbSt(s string) string {
	usStDict := map[string]string{
		"AK": "Alaska", "AL": "Alabama", "AR": "Arkansas", "AS": "American Samoa", "AZ": "Arizona",
		"CA": "California", "CO": "Colorado", "CT": "Connecticut", "DC": "District of Columbia", "DE": "Delaware",
		"FL": "Florida", "GA": "Georgia", "GU": "Guam", "HI": "Hawaii", "IA": "Iowa",
		"ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "KS": "Kansas", "KY": "Kentucky",
		"LA": "Louisiana", "MA": "Massachusetts", "MD": "Maryland", "ME": "Maine", "MI": "Michigan",
		"MN": "Minnesota", "MO": "Missouri", "MP": "Northern Mariana Islands", "MS": "Mississippi", "MT": "Montana",
		"NA": "National", "NC": "North Carolina", "ND": "North Dakota", "NE": "Nebraska", "NH": "New Hampshire",
		"NJ": "New Jersey", "NM": "New Mexico", "NV": "Nevada", "NY": "New York", "OH": "Ohio",
		"OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "PR": "Puerto Rico", "RI": "Rhode Island",
		"SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
		"VA": "Virginia", "VI": "Virgin Islands", "VT": "Vermont", "WA": "Washington", "WI": "Wisconsin",
		"WV": "West Virginia", "WY": "Wyoming"}
	if ds, ok := usStDict[s]; ok {
		return ds
	}
	return s
}
