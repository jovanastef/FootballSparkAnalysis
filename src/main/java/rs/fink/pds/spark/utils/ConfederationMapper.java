package rs.fink.pds.spark.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapira fudbalske reprezentacije u njihove kontinentalne konfederacije.
 * Potrebno za zadatak 3
 */
public class ConfederationMapper {
    
    private static final Map<String, String> CONFED_MAP = new HashMap<String, String>();
    
    static {
        // UEFA - Europa
        String[] uefa = {
        		// Balkan
                "Serbia", "FR Yugoslavia", "Yugoslavia",
                "Croatia", "Bosnia and Herzegovina", "North Macedonia", "Montenegro",
                "Slovenia", "Albania", "Kosovo",
                
                // Bivse drzave
                "Czech Republic", "Czechoslovakia", "Serbia and Montenegro", 
                "FR Yugoslavia", "Yugoslavia", "Macedonia",
                "Soviet Union", "CIS", 
                "East Germany", "West Germany", "Germany",
                
                // Zapadna Evropa
                "England", "Scotland", "Wales", "Northern Ireland", "Republic of Ireland",
                "France", "Spain", "Portugal", "Italy", "Netherlands", "Belgium",
                "Switzerland", "Austria", "Luxembourg", "Liechtenstein", "Monaco",
                "Andorra", "San Marino", "Malta", "Gibraltar","Slovakia",
                
                // Skandinavija
                "Denmark", "Sweden", "Norway", "Finland", "Iceland", "Faroe Islands",
                
                // Istocna Evropa
                "Russia", "Ukraine", "Belarus", "Poland", "Hungary", "Romania", 
                "Bulgaria", "Greece", "Turkey",
                "Estonia", "Latvia", "Lithuania", "Moldova", "Georgia",
                "Armenia", "Azerbaijan", "Kazakhstan", "Israel", "Cyprus"
        };
        for (String t : uefa) CONFED_MAP.put(t, "UEFA");
        
        // CONMEBOL - J. Amerika
        String[] conmebol = {"Brazil", "Argentina", "Uruguay", "Colombia", "Chile", 
                           "Paraguay", "Peru", "Ecuador", "Bolivia", "Venezuela"};
        for (String t : conmebol) CONFED_MAP.put(t, "CONMEBOL");
        
        // CONCACAF - Severna i srednja Amerika
        String[] concacaf = {
        		// Severna Amerika
                "United States", "Mexico", "Canada",
                
                // Srednja Amerika
                "Costa Rica", "Panama", "Honduras", "Guatemala", "El Salvador",
                "Nicaragua", "Belize",
                
                // Karibi
                "Jamaica", "Trinidad and Tobago", "Haiti", "Cuba",
                "Barbados", "Antigua and Barbuda", "Saint Kitts and Nevis",
                "Dominica", "Grenada", "Saint Lucia", "Saint Vincent and the Grenadines",
                "Bahamas", "Bermuda", "Cayman Islands", "Turks and Caicos Islands",
                "Curacao", "Aruba", "Guadeloupe", "Martinique", "Puerto Rico",
                "Dominican Republic", "Suriname", "Guyana", "French Guiana"
        };
        for (String t : concacaf) CONFED_MAP.put(t, "CONCACAF");
        
        // CAF - Afrika
        String[] caf = {
        		// Severna Afrika
                "Egypt", "Morocco", "Algeria", "Tunisia", "Libya", "Sudan",
                
                // Zapadna Afrika
                "Nigeria", "Senegal", "Ghana", "Ivory Coast", "Cameroon",
                "Mali", "Burkina Faso", "Guinea", "Sierra Leone", "Liberia",
                "Gambia", "Guinea-Bissau", "Cape Verde", "Mauritania",
                "Niger", "Chad", "Benin", "Togo",
                
                // Centralna Afrika
                "DR Congo", "Congo", "Central African Republic", "Gabon",
                "Equatorial Guinea", "Republic of the Congo", "Angola",
                "São Tomé and Príncipe", "Burundi", "Rwanda",
                
                // Istocna Afrika
                "Kenya", "Uganda", "Tanzania", "Ethiopia", "Eritrea",
                "Somalia", "Djibouti", "South Sudan",
                
                // Juzna Afrika
                "South Africa", "Zambia", "Zimbabwe", "Malawi", "Mozambique",
                "Namibia", "Botswana", "Lesotho", "Eswatini", "Swaziland",
                "Madagascar", "Mauritius", "Seychelles", "Comoros"
        };
        for (String t : caf) CONFED_MAP.put(t, "CAF");
        
        // AFC - Azija
        String[] afc = {
        		// Istocna Azija
                "Japan", "South Korea", "North Korea", "China PR", "Chinese Taipei",
                "Taiwan", "Hong Kong", "Macau", "Mongolia", "Guam",
                
                // Jugoistocna Azija
                "Thailand", "Vietnam", "Malaysia", "Singapore", "Indonesia",
                "Philippines", "Myanmar", "Cambodia", "Laos", "Brunei",
                "Timor-Leste",
                
                // Zapadna Azija (Srednji Istok)
                "Iran", "Saudi Arabia", "Iraq", "United Arab Emirates", "Qatar",
                "Kuwait", "Bahrain", "Oman", "Yemen", "Jordan",
                "Syria", "Lebanon", "Palestine", "Israel",
                
                // Centralna i Juzna Azija
                "Uzbekistan", "Kazakhstan", "Kyrgyzstan", "Tajikistan", "Turkmenistan",
                "Afghanistan", "Pakistan", "Bangladesh", "Sri Lanka", "Maldives",
                "Nepal", "Bhutan", "India"
        };
        for (String t : afc) CONFED_MAP.put(t, "AFC");
        
        // OFC - Okeanija
        String[] ofc = {"Australia", "New Zealand", "Fiji", "Papua New Guinea",
                "New Caledonia", "Tahiti", "French Polynesia", "Solomon Islands",
                "Vanuatu", "Samoa", "American Samoa", "Tonga", "Cook Islands",
                "Niue", "Kiribati", "Tuvalu", "Palau", "Micronesia"
        };
        for (String t : ofc) CONFED_MAP.put(t, "OFC");
    }
    
    /**
     * Vraca konfederaciju za dati tim.
     * @param team Ime reprezentacije
     * @return Naziv konfederacije ili "UNKNOWN"
     */
    public static String get(String team) {
        if (team == null || team.trim().isEmpty()) {
            return "UNKNOWN";
        }
        return CONFED_MAP.getOrDefault(team.trim(), "UNKNOWN");
    }
    
    /**
     * Proverava da li je tim poznat u mapi.
     * @param team Ime reprezentacije
     * @return true ako je poznat
     */
    public static boolean isKnown(String team) {
        return team != null && CONFED_MAP.containsKey(team.trim());
    }
    
    /**
     * Vraca sve timove iz odredjene konfederacije.
     * @param confederation Naziv konfederacije
     * @return Niz imena timova
     */
    public static String[] getTeamsByConfederation(String confederation) {
        java.util.List<String> teams = new java.util.ArrayList<String>();
        for (java.util.Map.Entry<String, String> entry : CONFED_MAP.entrySet()) {
            if (entry.getValue().equals(confederation)) {
                teams.add(entry.getKey());
            }
        }
        return teams.toArray(new String[0]);
    }
}