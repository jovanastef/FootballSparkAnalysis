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

<img width="357" height="274" alt="11" src="https://github.com/user-attachments/assets/448c26cf-0caa-459c-a4eb-c42ad220bab2" />



### Zadatak 2

Pronalaze se sve utakmice na kojima je u periodu od 60. minuta do kraja utakmice postignuto najmanje 4 gola.


<img width="553" height="424" alt="22" src="https://github.com/user-attachments/assets/6bf4841e-29b3-4407-88e0-10d1ad6fd21f" />


### Zadatak 3

Za FIFA World Cup odrzana od 1970. godine pa nadalje, odredjuje se po jedna reprezentacija za svaku konfederaciju 

koja je postigla najveci ukupan broj golova.


<img width="446" height="225" alt="33" src="https://github.com/user-attachments/assets/b701cb28-ebf0-44da-83cc-172db4d2faf2" />


### Zadatak 4

Odredjuje se 15 fudbalskih reprezentacija koje su nakon 1990. godine imale najduzi niz uzastopnih utakmica bez primljenog gola. 

U analizu su ukljucene samo reprezentacije koje su ucestvovale bar na jednom evropskom ili svetskom fudbalskom prvenstvu.

<img width="424" height="352" alt="44" src="https://github.com/user-attachments/assets/10ea110b-91b2-4504-a1cb-1e8fcf9dcbde" />


