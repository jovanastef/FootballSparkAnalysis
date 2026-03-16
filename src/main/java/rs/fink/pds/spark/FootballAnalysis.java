package rs.fink.pds.spark;

import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

public class FootballAnalysis implements Serializable {
 // RDD + Map-Reduce
 public static void zad1_SerbiaAnalysis(JavaRDD<String> resultsRDD) {
     System.out.println("\n========== ZADATAK 1: Srbija - RDD Analiza ==========");
     
     // Filter: samo utakmice gde je Srbija ucestvovala (preskacemo header)
     JavaRDD<String> serbiaMatches = resultsRDD
         .filter(line -> !line.startsWith("date,"))
         .filter(line -> {
             String[] p = line.split(",");
             if (p.length < 6) return false;
             String home = p[1].trim(), away = p[2].trim();
             return home.equals("Serbia") || away.equals("Serbia") ||
                    home.equals("Serbia and Montenegro") || away.equals("Serbia and Montenegro") ||
                    home.equals("FR Yugoslavia") || away.equals("FR Yugoslavia");
         });
     
     // Map: (opponent, (wins, draws, losses))
     JavaPairRDD<String, Tuple3<Integer,Integer,Integer>> mapped = serbiaMatches
         .mapToPair(new PairFunction<String, String, Tuple3<Integer,Integer,Integer>>() {
             @Override
             public Tuple2<String, Tuple3<Integer,Integer,Integer>> call(String line) {
                 String[] p = line.split(",");
                 String home = p[1].trim(), away = p[2].trim();
                 int homeScore = Integer.parseInt(p[3].trim());
                 int awayScore = Integer.parseInt(p[4].trim());
                 
                 // Odredi protivnika
                 String opponent;
                 boolean isSerbiaHome = home.equals("Serbia") || home.equals("Serbia and Montenegro") || home.equals("FR Yugoslavia");
                 if (isSerbiaHome) {
                     opponent = away;
                 } else {
                     opponent = home;
                 }
                 
                 // Odredi rezultat za Srbiju
                 int result;
                 if (isSerbiaHome) {
                     result = (homeScore > awayScore) ? 1 : (homeScore == awayScore) ? 0 : -1;
                 } else {
                     result = (awayScore > homeScore) ? 1 : (awayScore == homeScore) ? 0 : -1;
                 }
                 
                 Tuple3<Integer,Integer,Integer> stats = 
                     (result == 1) ? new Tuple3<>(1,0,0) :
                     (result == 0) ? new Tuple3<>(0,1,0) :
                                     new Tuple3<>(0,0,1);
                 return new Tuple2<>(opponent, stats);
             }
         });
     
     // Reduce: saberi statistike po protivniku
     JavaPairRDD<String, Tuple3<Integer,Integer,Integer>> reduced = mapped
         .reduceByKey(new Function2<Tuple3<Integer,Integer,Integer>, Tuple3<Integer,Integer,Integer>, Tuple3<Integer,Integer,Integer>>() {
             @Override
             public Tuple3<Integer,Integer,Integer> call(Tuple3<Integer,Integer,Integer> a, Tuple3<Integer,Integer,Integer> b) {
                 return new Tuple3<>(a._1()+b._1(), a._2()+b._2(), a._3()+b._3());
             }
         });
     
  // Finalni format: Team,Matches,Win%,Draw%,Loss%
     JavaRDD<String> output = reduced.map(
         new Function<Tuple2<String, Tuple3<Integer,Integer,Integer>>, String>() {
             @Override
             public String call(Tuple2<String, Tuple3<Integer,Integer,Integer>> e) {
                 String opp = e._1();
                 int w = e._2()._1(), d = e._2()._2(), l = e._2()._3();
                 int total = w + d + l;
                 
                 double wp = total > 0 ? Math.round(w * 1000.0 / total) / 10.0 : 0;
                 double dp = total > 0 ? Math.round(d * 1000.0 / total) / 10.0 : 0;
                 double lp = total > 0 ? Math.round(l * 1000.0 / total) / 10.0 : 0;
                 
                 return String.format("%s,%d,%.1f,%.1f,%.1f", opp, total, wp, dp, lp);
             }
         });
     
     // Sortiraj po broju utakmica i prikazi top 15
     System.out.println("Team,MatchesPlayed,WinPercentage,DrawPercentage,LossPercentage");
     List<String> top15 = output.sortBy(new Function<String, Integer>() {
         @Override
         public Integer call(String line) {
             return -Integer.parseInt(line.split(",")[1]);
         }
     }, true, 1).take(15);
     
     for (String line : top15) {
         System.out.println(line);
     }
 }
 
 public static void main(String[] args) {
     SparkSession spark = SparkSession.builder()
         .appName("FootballAnalysis")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.host", "localhost")
         .getOrCreate();
     
     spark.sparkContext().setLogLevel("WARN");
     
     // Ucitavanje podataka
     String resultsPath = "dataset/results.csv";
     String goalsPath = "dataset/goalcorers.csv";
     
     Dataset<Row> resultsDF = spark.read().option("header","true").csv(resultsPath);
     Dataset<Row> goalsDF = spark.read().option("header","true").csv(goalsPath);
     JavaRDD<String> resultsRDD = spark.sparkContext()
         .textFile(resultsPath, 4).toJavaRDD();
     
     System.out.println("========================================");
     System.out.println("PDS Football Spark Analysis - Starting");
     System.out.println("========================================");
     System.out.println("Results rows: " + resultsDF.count());
     System.out.println("Goalscorers rows: " + goalsDF.count());
     
     // Pokreni sve zadatke
     zad1_SerbiaAnalysis(resultsRDD);           // RDD + Map-Reduce
     
     spark.stop();
     System.out.println("\nAnalysis finished");
 }
}