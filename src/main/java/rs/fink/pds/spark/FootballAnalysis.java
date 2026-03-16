package rs.fink.pds.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import rs.fink.pds.spark.utils.ConfederationMapper;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;


import static org.apache.spark.sql.functions.*;

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
 
 
 //Utakmice sa 4+ gola nakon 60'
 
 public static void zad2_LateGoals(SparkSession spark, Dataset<Row> resultsDF, 
         							Dataset<Row> goalsDF) {
	 System.out.println("\n========== ZADATAK 2: golovi 60'+ ==========");

	// Eksplicitna konverzija minute u int
	Dataset<Row> goalsInt = goalsDF.withColumn("minute", col("minute").cast("int"));

	// Filtriraj golove nakon 60' (iskljuci autogolove)
	Dataset<Row> late = goalsInt
			.filter(col("minute").geq(60))
			.filter(col("own_goal").equalTo("FALSE"));
	
	// Grupisi po utakmici i prebroj kasne golove
	Dataset<Row> lateCount = late
			.groupBy("date", "home_team", "away_team")
			.agg(count("*").as("LateGoals"))
			.filter(col("LateGoals").geq(4));
	
	// alias za dataframes
	Dataset<Row> resultsAliased = resultsDF.alias("r");
    Dataset<Row> lateCountAliased = lateCount.alias("lc");
	
    // Spoj sa rezultatima za dodatne informacije
	Dataset<Row> result = lateCountAliased
			.join(resultsAliased, 
					lateCountAliased.col("date").equalTo(resultsAliased.col("date"))
					.and(lateCountAliased.col("home_team").equalTo(resultsAliased.col("home_team")))
					.and(lateCountAliased.col("away_team").equalTo(resultsAliased.col("away_team"))),
					"inner")
			//.withColumn("TotalGoals", col("home_score").cast("int").plus(col("away_score").cast("int")))
			.select(
					resultsAliased.col("date").as("Date"),
					resultsAliased.col("home_team").as("HomeTeam"), 
					resultsAliased.col("away_team").as("AwayTeam"),
					resultsAliased.col("tournament").as("Tournament"),
					lateCountAliased.col("LateGoals"),
					(resultsAliased.col("home_score").cast("int")
			                .plus(resultsAliased.col("away_score").cast("int"))).as("TotalGoals")
			        )
			
			.orderBy(col("LateGoals").desc(), col("Date").desc());
	
	System.out.println("Date,HomeTeam,AwayTeam,Tournament,LateGoals,TotalGoals");
	result.show(30, false);
	result.write().mode("overwrite").option("header", "true").csv("output/zad2_late_goals");
}

 //Najefikasnije reprezentacije po konfederacijama na SP 1970+
 public static void zad3_WorldCupByConfed(SparkSession spark, Dataset<Row> resultsDF,
		 									Dataset<Row> goalsDF) {
	 System.out.println("\n========== ZADATAK 3: SP po konfederacijama ==========");
	 
	 StructType resultSchema = new StructType(new StructField[] {
			    DataTypes.createStructField("Confederation", DataTypes.StringType, false),
			    DataTypes.createStructField("Team", DataTypes.StringType, false),
			    DataTypes.createStructField("WorldCupGoals", DataTypes.LongType, false),
			    DataTypes.createStructField("FirstAppearanceYear", DataTypes.IntegerType, false),
			    DataTypes.createStructField("LastAppearanceYear", DataTypes.IntegerType, false)
	 });
	 
	// Filtriraj samo FIFA World Cup utakmice od 1970. (ukljucujuci kvalifikacije)
	Dataset<Row> wcMatches = resultsDF
			.filter(col("tournament").contains("FIFA World Cup"))
			.withColumn("year", year(col("date")))
			.filter(col("year").geq(1970));
	System.out.println("World Cup utakmice od 1970: " + wcMatches.count());
	// Spoji golove sa SP utakmicama
	Dataset<Row> wcGoals = goalsDF
			.withColumn("minute", col("minute").cast("int"))
			.join(wcMatches,
					goalsDF.col("date").equalTo(wcMatches.col("date"))
					.and(goalsDF.col("home_team").equalTo(wcMatches.col("home_team")))
					.and(goalsDF.col("away_team").equalTo(wcMatches.col("away_team"))),
					"inner")
			.filter(col("own_goal").equalTo("FALSE"));
	System.out.println("Golovi na SP od 1970: " + wcGoals.count());
	
	// Ukupni golovi po timu na SP
	Dataset<Row> teamGoals = wcGoals
			.groupBy("team")
			.agg(
					count("*").as("WorldCupGoals"),
					min(year(col("date"))).as("FirstAppearanceYear"),
					max(year(col("date"))).as("LastAppearanceYear")
			);
	System.out.println("Timovi sa golovima na SP: " + teamGoals.count());
	
	// Dodaj konfederaciju

	Dataset<Row> withConfed = teamGoals.map(new MapFunction<Row, Row>() {
	    @Override
	    public Row call(Row row) throws Exception {
	        String team = row.getString(0);
	        long goals = row.getLong(1);
            int firstYear = row.getInt(2);
            int lastYear = row.getInt(3);
            
	        String confed = ConfederationMapper.get(team);
	        if ("UNKNOWN".equals(confed)) {
	        	return null;
	        }
	        return RowFactory.create(confed, team, goals, firstYear, lastYear);
	    }
	}, Encoders.row(resultSchema)) 
	.filter((FilterFunction<Row>) r -> r != null);
	
	// Najbolji tim po konfederaciji (window funkcija)
	WindowSpec window = Window.partitionBy("Confederation")
							.orderBy(col("WorldCupGoals").desc());
	
	Dataset<Row> ranked = withConfed
			.withColumn("rank", row_number().over(window))
			.filter(col("rank").equalTo(1))
			.select("Confederation", "Team", "WorldCupGoals", 
					"FirstAppearanceYear", "LastAppearanceYear")
			.orderBy(col("WorldCupGoals").desc());
	
	System.out.println("Confederation,Team,WorldCupGoals,FirstAppearanceYear,LastAppearanceYear");
	ranked.show(10, false);
	ranked.write().mode("overwrite").option("header", "true").csv("output/zad3_worldcup_confed");
	
	System.out.println("\nRezultati sacuvani u: output/zad3_worldcup_confed/");
}
 

// Najduzi nizovi utakmica bez primljenog gola (nakon 1990)

public static void zad4_CleanSheets(SparkSession spark, Dataset<Row> resultsDF) {
  System.out.println("\n========== ZADATAK 4: Clean Sheet nizovi ==========");

  // Filtriraj utakmice nakon 1990.
  Dataset<Row> filtered = resultsDF
      .filter(year(col("date")).geq(1990))
      .filter(
          col("tournament").contains("FIFA World Cup")
          .or(col("tournament").contains("UEFA Euro"))
      );
  
  System.out.println("Utakmice na SP/EP nakon 1990: " + filtered.count());
  
  // Timovi koji su igrali na SP ili EP (bilo kada)
  Dataset<Row> majorTeams = resultsDF
      .filter(
          col("tournament").contains("FIFA World Cup")
          .or(col("tournament").contains("UEFA Euro"))
      )
      .select(col("home_team").as("team"))
      .union(resultsDF.select(col("away_team").as("team")))
      .distinct();
  
  System.out.println("Timovi sa SP/EP: " + majorTeams.count());
  
  // Za svaki tim, da li je primio gol na svakoj utakmici
  
  Dataset<Row> teamResults = filtered
      .select(
          col("date"),
          col("home_team"),
          col("away_team"),
          col("home_score").cast("int"),
          col("away_score").cast("int")
      )
      .withColumn("home_clean", 
          when(col("away_score").cast("int").equalTo(0), true).otherwise(false))
      .withColumn("away_clean", 
          when(col("home_score").cast("int").equalTo(0), true).otherwise(false));
  
  // jedan red po timu po utakmici
  Dataset<Row> teamCleanHome = teamResults
      .select(
          col("date"),
          col("home_team").as("team"),
          col("home_clean").as("clean"),
          col("away_team").as("opponent")
      );
  
  Dataset<Row> teamCleanAway = teamResults
      .select(
          col("date"),
          col("away_team").as("team"),
          col("away_clean").as("clean"),
          col("home_team").as("opponent")
      );
  
  Dataset<Row> teamClean = teamCleanHome
      .union(teamCleanAway)
      .join(majorTeams, "team", "inner")
      .orderBy("team", "date");
  
  System.out.println("Ukupno tim-utakmica zapisa: " + teamClean.count());
  
  
  // privremen view za SQL upit
  teamClean.createOrReplaceTempView("clean_sheets");
  
  // SQL za pronalazenje uzastopnih nizova
  String sql = 
      "WITH streaks AS (" +
      "    SELECT team, date, clean, opponent, " +
      "        SUM(CASE WHEN clean = false THEN 1 ELSE 0 END) " +
      "            OVER (PARTITION BY team ORDER BY date) as grp " +
      "    FROM clean_sheets " +
      "), " +
      "summary AS (" +
      "    SELECT team, grp, COUNT(*) as len, " +
      "        MIN(date) as start_d, MAX(date) as end_d, " +
      "        MAX(CASE WHEN clean = false THEN opponent END) as broken " +
      "    FROM streaks WHERE clean = true " +
      "    GROUP BY team, grp " +
      ") " +
      "SELECT team as Team, len as CleanSheetStreak, " +
      "    start_d as StartDate, end_d as EndDate, " +
      "    broken as StreakBrokenBy " +
      "FROM summary ORDER BY len DESC, team LIMIT 15";
  
  Dataset<Row> top = spark.sql(sql);
  
  System.out.println("\nTeam,CleanSheetStreak,StartDate,EndDate,StreakBrokenBy");
  top.show(15, false);
  
  //Sacuvaj rezultate u CSV fajl
  top.write()
      .mode("overwrite")
      .option("header", "true")
      .csv("output/zad4_cleansheets");
  
  System.out.println("\nRezultati sacuvani u: output/zad4_cleansheets/");
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
     String goalsPath = "dataset/goalscorers.csv";
     
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
     zad2_LateGoals(spark, resultsDF, goalsDF); // DataFrame
     zad3_WorldCupByConfed(spark, resultsDF, goalsDF);
     zad4_CleanSheets(spark, resultsDF);
     
     spark.stop();
     System.out.println("\nAnalysis finished");
 }
}