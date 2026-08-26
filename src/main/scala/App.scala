import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // Spark Configuration
    // ============================================================

    val spark = SparkSession.builder()
  .appName("Spark Exercises 1-20")
  .master("local[*]")
  .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
  .getOrCreate()

    val sc = spark.sparkContext

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("\n============================================================")
    println("        APACHE SPARK EXERCISES 1 - 20")
    println("============================================================")


    // ============================================================
    // EXERCISE 1 - Spark Cluster / Basic Spark Application
    // ============================================================

    println("\n---------------- EXERCISE 1 ----------------")
    println("Spark Application Information")

    println("Application Name : " + spark.conf.get("spark.app.name"))
    println("Spark Version    : " + spark.version)
    println("Master           : " + sc.master)
    println("Application ID   : " + sc.applicationId)


    // ============================================================
    // EXERCISE 2 - Spark vs Hadoop MapReduce
    // ============================================================

    println("\n---------------- EXERCISE 2 ----------------")
    println("Spark vs Hadoop MapReduce")

    println("Spark:")
    println("- In-memory processing")
    println("- Supports RDD, DataFrame and SQL")
    println("- Suitable for iterative processing")
    println("- Supports batch and streaming")

    println("\nHadoop MapReduce:")
    println("- Disk-based intermediate processing")
    println("- Map and Reduce programming model")
    println("- More disk I/O")
    println("- Primarily designed for batch processing")


    // ============================================================
    // EXERCISE 3 - Create First RDD
    // ============================================================

    println("\n---------------- EXERCISE 3 ----------------")
    println("First RDD")

    val numbersRDD = sc.parallelize(1 to 10)

    println("RDD Contents:")
    numbersRDD.collect().foreach(println)

    println("Number of elements: " + numbersRDD.count())


    // ============================================================
    // EXERCISE 4 - RDD Immutability and Lineage
    // ============================================================

    println("\n---------------- EXERCISE 4 ----------------")
    println("RDD Immutability and Lineage")

    val originalRDD = sc.parallelize(1 to 10)

    val evenRDD = originalRDD.filter(_ % 2 == 0)

    println("Original RDD:")
    originalRDD.collect().foreach(println)

    println("Transformed RDD:")
    evenRDD.collect().foreach(println)

    println("\nLineage:")
    println(evenRDD.toDebugString)


    // ============================================================
    // EXERCISE 5 - Word Count
    // ============================================================

    println("\n---------------- EXERCISE 5 ----------------")
    println("RDD Word Count")

    val text = Seq(
      "spark is fast",
      "spark is powerful",
      "spark is easy",
      "big data uses spark"
    )

    val linesRDD = sc.parallelize(text)

    val wordCounts = linesRDD
      .flatMap(_.split("\\s+"))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)
      .sortByKey()

    println("Word Count:")

    wordCounts.collect().foreach {
      case (word, count) =>
        println(word + " -> " + count)
    }


    // ============================================================
    // EXERCISE 6 - Transformations vs Actions
    // ============================================================

    println("\n---------------- EXERCISE 6 ----------------")
    println("Transformations vs Actions")

    val transformationRDD = sc.parallelize(1 to 10)

    val transformed = transformationRDD
      .map(_ * 2)
      .filter(_ > 10)

    println("Transformation created.")

    println("Action - collect:")
    transformed.collect().foreach(println)

    println("Action - count:")
    println(transformed.count())


    // ============================================================
    // EXERCISE 7 - Narrow and Wide Transformations
    // ============================================================

    println("\n---------------- EXERCISE 7 ----------------")
    println("Narrow and Wide Transformations")

    val dataRDD = sc.parallelize(
      Seq(
        ("A", 10),
        ("B", 20),
        ("A", 30),
        ("B", 40)
      )
    )

    val narrowRDD = dataRDD.map {
      case (key, value) => (key, value * 2)
    }

    println("Narrow Transformation Result:")
    narrowRDD.collect().foreach(println)

    val wideRDD = dataRDD.reduceByKey(_ + _)

    println("\nWide Transformation Result:")
    wideRDD.collect().foreach(println)


    // ============================================================
    // EXERCISE 8 - Pair RDD
    // ============================================================

    println("\n---------------- EXERCISE 8 ----------------")
    println("Pair RDD Operations")

    val employeeRDD = sc.parallelize(
      Seq(
        ("Engineering", 95000),
        ("Sales", 72000),
        ("Engineering", 88000),
        ("Sales", 65000),
        ("Engineering", 105000)
      )
    )

    println("Employee Salary Data:")
    employeeRDD.collect().foreach(println)

    val departmentTotal = employeeRDD.reduceByKey(_ + _)

    println("\nTotal Salary by Department:")
    departmentTotal.collect().foreach(println)


    // ============================================================
    // EXERCISE 9 - reduce vs reduceByKey
    // ============================================================

    println("\n---------------- EXERCISE 9 ----------------")
    println("reduce vs reduceByKey")

    val salaryRDD = sc.parallelize(Seq(10000, 20000, 30000))

    val totalSalary = salaryRDD.reduce(_ + _)

    println("reduce result:")
    println(totalSalary)

    val salariesByDepartment = sc.parallelize(
      Seq(
        ("Engineering", 95000),
        ("Engineering", 88000),
        ("Sales", 72000),
        ("Sales", 65000)
      )
    )

    val departmentSalary = salariesByDepartment.reduceByKey(_ + _)

    println("\nreduceByKey result:")
    departmentSalary.collect().foreach(println)


    // ============================================================
    // EXERCISE 10 - Spark Execution Flow
    // ============================================================

    println("\n---------------- EXERCISE 10 ----------------")
    println("Spark Execution Flow")

    println("Driver Program")
    println("      |")
    println("      v")
    println("SparkContext")
    println("      |")
    println("      v")
    println("RDD Transformations")
    println("      |")
    println("      v")
    println("Action")
    println("      |")
    println("      v")
    println("Job")
    println("      |")
    println("      v")
    println("Stages")
    println("      |")
    println("      v")
    println("Tasks")
    println("      |")
    println("      v")
    println("Executors")


    // ============================================================
    // EXERCISE 11 - DAG and Stages
    // ============================================================

    println("\n---------------- EXERCISE 11 ----------------")
    println("DAG and Stages")

    val dagRDD = sc.parallelize(1 to 20)

    val dagResult = dagRDD
      .map(_ * 2)
      .filter(_ % 4 == 0)
      .map(x => (x % 3, x))
      .reduceByKey(_ + _)

    println("DAG Result:")
    dagResult.collect().foreach(println)

    println("\nRDD Lineage / DAG:")
    println(dagResult.toDebugString)


    // ============================================================
    // EXERCISE 12 - Broadcast Variable
    // ============================================================

    println("\n---------------- EXERCISE 12 ----------------")
    println("Broadcast Variable")

    val taxRates = Map(
      "Engineering" -> 0.10,
      "Sales" -> 0.08
    )

    val broadcastTaxRates = sc.broadcast(taxRates)

    val employeeData = sc.parallelize(
      Seq(
        ("Rakesh", "Engineering", 95000),
        ("Kavish", "Sales", 72000),
        ("Chetan", "Engineering", 88000),
        ("Rahul", "Sales", 65000),
        ("Arjun", "Engineering", 105000)
      )
    )

    val afterTax = employeeData.map {
      case (name, dept, salary) =>
        val rate = broadcastTaxRates.value.getOrElse(dept, 0.0)
        val tax = salary * rate
        (name, dept, salary, tax)
    }

    println("Broadcast Variable Result:")

    afterTax.collect().foreach {
      case (name, dept, salary, tax) =>
        println(
          name +
            " | " +
            dept +
            " | Salary: " +
            salary +
            " | Tax: " +
            tax
        )
    }


    // ============================================================
    // EXERCISE 13 - Accumulator
    // ============================================================

    println("\n---------------- EXERCISE 13 ----------------")
    println("Accumulator")

    val invalidRecords = sc.longAccumulator("Invalid Records")

    val records = sc.parallelize(
      Seq(
        "100",
        "200",
        "invalid",
        "300",
        "wrong",
        "400"
      )
    )

    val validRecords = records.flatMap { value =>
      try {
        Some(value.toInt)
      } catch {
        case _: NumberFormatException =>
          invalidRecords.add(1)
          None
      }
    }

    println("Valid Records:")
    validRecords.collect().foreach(println)

    println("Invalid Records: " + invalidRecords.value)


    // ============================================================
    // EXERCISE 14 - Partitions
    // ============================================================

    println("\n---------------- EXERCISE 14 ----------------")
    println("RDD Partitions")

    val partitionRDD = sc.parallelize(1 to 20, 4)

    println("Number of partitions: " + partitionRDD.getNumPartitions)

    val partitionContents = partitionRDD.mapPartitionsWithIndex {
      case (partitionIndex, iterator) =>
        Iterator(
          "Partition " +
            partitionIndex +
            ": " +
            iterator.mkString(", ")
        )
    }

    partitionContents.collect().foreach(println)


    // ============================================================
    // EXERCISE 15 - repartition vs coalesce
    // ============================================================

    println("\n---------------- EXERCISE 15 ----------------")
    println("repartition vs coalesce")

    val originalPartitions = sc.parallelize(1 to 20, 2)

    println(
      "Original partitions: " +
        originalPartitions.getNumPartitions
    )

    val repartitioned = originalPartitions.repartition(4)

    println(
      "After repartition(4): " +
        repartitioned.getNumPartitions
    )

    val coalesced = repartitioned.coalesce(2)

    println(
      "After coalesce(2): " +
        coalesced.getNumPartitions
    )

    println("repartition can increase/decrease partitions and causes shuffle.")
    println("coalesce is generally used to reduce partitions with less shuffle.")


    // ============================================================
    // EXERCISE 16 - Cache and Persist
    // ============================================================

    println("\n---------------- EXERCISE 16 ----------------")
    println("Cache and Persist")

    val cacheRDD = sc.parallelize(1 to 1000000)
      .map(_ * 2)
      .filter(_ % 4 == 0)

    cacheRDD.cache()

    println("First action:")
    println(cacheRDD.count())

    println("Second action:")
    println(cacheRDD.count())

    cacheRDD.unpersist()

    println("RDD cache removed.")


    // ============================================================
    // EXERCISE 17 - Spark on YARN
    // ============================================================

    println("\n---------------- EXERCISE 17 ----------------")
    println("Spark on YARN")

    println("Spark can run on YARN using:")
    println("1. Client mode")
    println("2. Cluster mode")

    println("\nClient Mode:")
    println("Driver runs on the client machine.")

    println("\nCluster Mode:")
    println("Driver runs inside the cluster.")

    println("\nCurrent application master:")
    println(sc.master)


    // ============================================================
    // EXERCISE 18 - Spark SQL / DataFrame
    // ============================================================

    println("\n---------------- EXERCISE 18 ----------------")
    println("Spark SQL / DataFrame")

    val employeeDF = Seq(
      ("Rakesh", "Engineering", 95000),
      ("Kavish", "Sales", 72000),
      ("Chetan", "Engineering", 88000),
      ("Rahul", "Sales", 65000),
      ("Arjun", "Engineering", 105000)
    ).toDF("name", "dept", "salary")

    println("Employee DataFrame:")
    employeeDF.show()

    println("Employees with salary greater than 80000:")

    employeeDF
      .filter($"salary" > 80000)
      .show()

    println("Average Salary by Department:")

    employeeDF
      .groupBy("dept")
      .agg(avg("salary").alias("average_salary"))
      .show()


    // ============================================================
    // EXERCISE 19 - DataFrame Joins
    // ============================================================

    println("\n---------------- EXERCISE 19 ----------------")
    println("DataFrame Joins")

    val employeeInfo = Seq(
      (1, "Rakesh", 101),
      (2, "Kavish", 102),
      (3, "Chetan", 101),
      (4, "Rahul", 102),
      (5, "Arjun", 101)
    ).toDF("employee_id", "name", "department_id")

    val departmentInfo = Seq(
      (101, "Engineering"),
      (102, "Sales")
    ).toDF("department_id", "department_name")

    println("Employee Data:")
    employeeInfo.show()

    println("Department Data:")
    departmentInfo.show()

    println("Inner Join Result:")

    val joinedDF = employeeInfo
      .join(
        departmentInfo,
        employeeInfo("department_id") === departmentInfo("department_id"),
        "inner"
      )
      .select(
        employeeInfo("employee_id"),
        employeeInfo("name"),
        departmentInfo("department_name")
      )

    joinedDF.show()


    // ============================================================
    // EXERCISE 20 - End-to-End Spark Project
    // ============================================================

    println("\n---------------- EXERCISE 20 ----------------")
    println("End-to-End Employee Salary Analysis")

    val finalDF = Seq(
      ("Rakesh", "Engineering", 95000),
      ("Kavish", "Sales", 72000),
      ("Chetan", "Engineering", 88000),
      ("Rahul", "Sales", 65000),
      ("Arjun", "Engineering", 105000),
      ("Suresh", "HR", 60000),
      ("Priya", "HR", 68000)
    ).toDF("name", "dept", "salary")

    println("Original Employee Data:")
    finalDF.show()

    println("Employees with salary >= 70000:")

    val highSalaryEmployees = finalDF
      .filter($"salary" >= 70000)

    highSalaryEmployees.show()

    println("Salary after 10% bonus:")

    val bonusDF = finalDF
      .withColumn(
        "salary_after_bonus",
        round($"salary" * 1.10, 2)
      )

    bonusDF.show()

    println("Average Salary by Department:")

    val averageSalaryDF = finalDF
      .groupBy("dept")
      .agg(
        round(avg("salary"), 2)
          .alias("average_salary")
      )
      .orderBy(desc("average_salary"))

    averageSalaryDF.show()

    println("Employee Count by Department:")

    val employeeCountDF = finalDF
      .groupBy("dept")
      .count()
      .orderBy(desc("count"))

    employeeCountDF.show()

    println("Highest Paid Employee:")

    finalDF
      .orderBy(desc("salary"))
      .limit(1)
      .show()

    println("\n============================================================")
    println("        ALL 20 EXERCISES COMPLETED")
    println("============================================================")

    spark.stop()
  }
}
