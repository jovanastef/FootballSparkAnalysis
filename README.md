# Football Spark Analysis

Apache Spark projekat za analizu fudbalskih 
utakmica fudbalskih reprezentacija od 1872. godine do danas.

## O projektu

Implementirana su 4 razlicita zadatka koriscenjem RDD, DataFrame i Spark SQL pristupa.

Programski jezik: Java 1.8

Spark verzija: 3.5.8

Scala verzija: 2.13

Build tool: Maven

## Pokretanje

mvn clean package

1. Otvori FootballAnalysis.java

2. Desni klik -> Run As -> Java Application

### Zadatak 1

Odredjuje se sa kojim fudbalskim reprezentacijama je Srbija odigrala najveci ukupan broj utakmica, 

a zatim se za te reprezentacije izracunava procenat pobeda, neresenih ishoda i poraza Srbije.
========================================
PDS Football Spark Analysis - Starting
========================================
Results rows: 49071
Goalscorers rows: 47555

========== ZADATAK 1: Srbija - RDD Analiza ==========
Team,MatchesPlayed,WinPercentage,DrawPercentage,LossPercentage
Russia,13,15.4,46.2,38.5
Lithuania,11,90.9,0.0,9.1
Slovenia,9,11.1,77.8,11.1
Republic of Ireland,9,44.4,44.4,11.1
South Korea,9,33.3,44.4,22.2
Spain,8,0.0,50.0,50.0
Portugal,8,12.5,50.0,37.5
Japan,8,50.0,0.0,50.0
Switzerland,8,25.0,50.0,25.0
Faroe Islands,8,100.0,0.0,0.0
Romania,7,57.1,28.6,14.3
Austria,7,57.1,14.3,28.6
Hungary,7,42.9,14.3,42.9
Poland,7,0.0,42.9,57.1
Belgium,7,28.6,14.3,57.1


### Zadatak 2

Pronalaze se sve utakmice na kojima je u periodu od 60. minuta do kraja utakmice postignuto najmanje 4 gola.


========== ZADATAK 2: golovi 60'+ ==========
Date,HomeTeam,AwayTeam,Tournament,LateGoals,TotalGoals
+----------+-----------+--------------------------------+----------------------------+---------+----------+
|Date      |HomeTeam   |AwayTeam                        |Tournament                  |LateGoals|TotalGoals|
+----------+-----------+--------------------------------+----------------------------+---------+----------+
|2001-04-11|Australia  |American Samoa                  |FIFA World Cup qualification|10       |31        |
|2001-04-09|Australia  |Tonga                           |FIFA World Cup qualification|9        |22        |
|1997-06-09|Maldives   |Syria                           |FIFA World Cup qualification|9        |12        |
|2021-03-30|Japan      |Mongolia                        |FIFA World Cup qualification|8        |14        |
|2000-06-19|Australia  |Cook Islands                    |Oceania Nations Cup         |8        |17        |
|1997-06-11|Australia  |Solomon Islands                 |FIFA World Cup qualification|8        |13        |
|1997-06-02|Maldives   |Iran                            |FIFA World Cup qualification|8        |17        |
|1993-05-03|Macau      |Kuwait                          |FIFA World Cup qualification|8        |11        |
|2023-11-18|France     |Gibraltar                       |UEFA Euro qualification     |7        |14        |
|2003-12-03|Maldives   |Mongolia                        |FIFA World Cup qualification|7        |12        |
|2001-02-28|Belgium    |San Marino                      |FIFA World Cup qualification|7        |11        |
|2000-11-26|Tajikistan |Guam                            |FIFA World Cup qualification|7        |16        |
|2000-11-24|Iran       |Guam                            |FIFA World Cup qualification|7        |19        |
|1998-09-28|Australia  |Cook Islands                    |Oceania Nations Cup         |7        |16        |
|1983-12-21|Spain      |Malta                           |UEFA Euro qualification     |7        |13        |
|2021-11-15|Niger      |Djibouti                        |FIFA World Cup qualification|6        |9         |
|2019-11-18|Italy      |Armenia                         |UEFA Euro qualification     |6        |10        |
|2016-09-06|Bulgaria   |Luxembourg                      |FIFA World Cup qualification|6        |7         |
|2012-06-01|Samoa      |Tahiti                          |Oceania Nations Cup         |6        |11        |
|2012-02-29|Bahrain    |Indonesia                       |FIFA World Cup qualification|6        |10        |
|2011-09-02|Netherlands|San Marino                      |UEFA Euro qualification     |6        |11        |
|2010-01-10|Angola     |Mali                            |African Cup of Nations      |6        |8         |
|2008-02-06|El Salvador|Anguilla                        |FIFA World Cup qualification|6        |12        |
|2006-09-06|San Marino |Germany                         |UEFA Euro qualification     |6        |13        |
|2004-07-07|Argentina  |Ecuador                         |Copa América                |6        |7         |
|1997-10-11|Germany    |Albania                         |FIFA World Cup qualification|6        |7         |
|1996-11-17|Honduras   |Saint Vincent and the Grenadines|FIFA World Cup qualification|6        |14        |
|1982-06-15|Hungary    |El Salvador                     |FIFA World Cup              |6        |11        |
|1981-08-14|Australia  |Fiji                            |FIFA World Cup qualification|6        |10        |
|1972-11-01|Netherlands|Norway                          |FIFA World Cup qualification|6        |9         |
+----------+-----------+--------------------------------+----------------------------+---------+----------+
only showing top 30 rows

### Zadatak 3

Za FIFA World Cup odrzana od 1970. godine pa nadalje, odredjuje se po jedna reprezentacija za svaku konfederaciju 

koja je postigla najveci ukupan broj golova.


========== ZADATAK 3: SP po konfederacijama ==========
World Cup utakmice od 1970: 8918
Golovi na SP od 1970: 24552
Timovi sa golovima na SP: 215
Confederation,Team,WorldCupGoals,FirstAppearanceYear,LastAppearanceYear
+-------------+---------+-------------+-------------------+------------------+
|Confederation|Team     |WorldCupGoals|FirstAppearanceYear|LastAppearanceYear|
+-------------+---------+-------------+-------------------+------------------+
|UEFA         |Germany  |422          |1970               |2025              |
|OFC          |Australia|419          |1973               |2025              |
|CONMEBOL     |Brazil   |415          |1970               |2025              |
|AFC          |Japan    |361          |1973               |2025              |
|CONCACAF     |Mexico   |330          |1970               |2022              |
|CAF          |Tunisia  |213          |1972               |2025              |
+-------------+---------+-------------+-------------------+------------------+


Rezultati sacuvani u: output/zad3_worldcup_confed/

### Zadatak 4

Odredjuje se 15 fudbalskih reprezentacija koje su nakon 1990. godine imale najduzi niz uzastopnih utakmica bez primljenog gola. 

U analizu su ukljucene samo reprezentacije koje su ucestvovale bar na jednom evropskom ili svetskom fudbalskom prvenstvu.


========== ZADATAK 4: Clean Sheet nizovi ==========
Utakmice na SP/EP nakon 1990: 9950
Timovi sa SP/EP: 318
Ukupno tim-utakmica zapisa: 19900

Team,CleanSheetStreak,StartDate,EndDate,StreakBrokenBy
+-----------+----------------+----------+----------+--------------------+
|Team       |CleanSheetStreak|StartDate |EndDate   |StreakBrokenBy      |
+-----------+----------------+----------+----------+--------------------+
|Iran       |12              |2015-11-17|2017-08-31|Syria               |
|Tunisia    |11              |2022-11-30|2025-10-13|NULL                |
|Ivory Coast|10              |2023-11-17|2025-10-14|NULL                |
|Netherlands|10              |2004-11-17|2006-06-11|Ivory Coast         |
|Spain      |10              |2014-10-12|2016-06-17|Croatia             |
|Mali       |9               |2017-09-05|2021-11-14|Tunisia             |
|Canada     |8               |2011-09-06|2012-09-07|Panama              |
|England    |8               |2025-03-21|2025-11-16|NULL                |
|Japan      |8               |2015-06-16|2016-03-29|United Arab Emirates|
|Japan      |8               |2023-11-16|2024-10-10|Australia           |
|Portugal   |8               |2009-09-09|2010-06-25|Spain               |
|Australia  |7               |2008-09-10|2009-06-10|Japan               |
|Belgium    |7               |2019-03-24|2019-10-13|Russia              |
|Cameroon   |7               |2000-04-19|2001-04-22|Angola              |
|Denmark    |7               |1992-06-26|1993-04-14|Republic of Ireland |
+-----------+----------------+----------+----------+--------------------+


Rezultati sacuvani u: output/zad4_cleansheets/

Analysis finished